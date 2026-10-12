import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIGURATION
# ============================================================

MF_CODES_FILE = "MFCodes.csv"

MAX_WORKERS = 6
TIMEOUT = 30

# HTTP retry configuration for transient errors
MAX_HTTP_RETRIES = 3
RETRY_BACKOFF = 1.5

# Delay between API calls
MIN_API_DELAY = 2.0
MAX_API_DELAY = 10.0

# Delay before retrying a failed scheme code
MIN_FAILED_RETRY_DELAY = 10.0
MAX_FAILED_RETRY_DELAY = 20.0

BASE_URL = (
    "https://www.rupeevest.com/home/"
    "get_mf_portfolio_tracker?schemecode={}"
)

DATA_KEYS = (
    "fund_info",
    "stock_data",
    "stock_mapping",
)

STOCK_DATA_KEYS = (
    "stock_data",
    "stock_data_debt",
    "stock_data_cash",
    "stock_data_misc",
)

MAPPING_KEYS = (
    "stock_mapping",
    "stock_mapping_debt",
    "stock_mapping_cash",
    "stock_mapping_misc",
)

OUTPUT_FILES = {
    "fund_info": "fund_info.csv",
    "stock_data": "stock_data.csv",
    "stock_mapping": "stock_mapping.csv",
}

logging.basicConfig(
    filename="log.txt",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# HTTP SESSION
# ============================================================

def make_session():
    """Create a requests session with connection pooling and HTTP retries."""
    session = requests.Session()

    retry = Retry(
        total=MAX_HTTP_RETRIES,
        connect=MAX_HTTP_RETRIES,
        read=MAX_HTTP_RETRIES,
        status=MAX_HTTP_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# ============================================================
# JSON HELPERS
# ============================================================

def flatten_dict_records(value):
    """
    Convert API sections containing either:

        [{"a": 1}, {"a": 2}]

    or:

        [[{"a": 1}, {"a": 2}]]

    or:

        [[{"a": 1}], [{"a": 2}]]

    into:

        [{"a": 1}, {"a": 2}]
    """
    records = []

    def walk(item):
        if isinstance(item, dict):
            records.append(item)

        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(value)
    return records


def records_to_df(value):
    """Convert a nested API section into a flat DataFrame."""
    records = flatten_dict_records(value)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def latest_entries(value):
    """
    Flatten a stock-data section and retain only rows belonging
    to the latest invdate returned by the API.
    """
    df = records_to_df(value)

    if df.empty:
        return df

    if "invdate" not in df.columns:
        return df.reset_index(drop=True)

    dates = pd.to_datetime(
        df["invdate"].astype(str).str.split("T").str[0],
        errors="coerce",
    )

    valid = dates.notna()

    if not valid.any():
        return pd.DataFrame()

    df = df.loc[valid].copy()
    dates = dates.loc[df.index]

    df["_invdate"] = dates
    latest_date = df["_invdate"].max()

    df = (
        df.loc[df["_invdate"] == latest_date]
        .drop(columns="_invdate")
        .reset_index(drop=True)
    )

    return df


def mapping_df(mapping, scheme_code):
    """Convert a mapping dictionary into a DataFrame."""
    if not isinstance(mapping, dict) or not mapping:
        return pd.DataFrame()

    df = pd.DataFrame(
        list(mapping.items()),
        columns=["fincode", "name"],
    )

    df["schemecode"] = scheme_code

    return df


# ============================================================
# API PROCESSING
# ============================================================

def fetch_scheme(scheme_code):
    """
    Fetch one scheme's complete API response.

    Returns:
        scheme_code, JSON response, error
    """
    session = make_session()

    try:
        delay = random.uniform(MIN_API_DELAY, MAX_API_DELAY)
        time.sleep(delay)

        url = BASE_URL.format(scheme_code)

        response = session.get(
            url,
            timeout=TIMEOUT,
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            raise ValueError("API response is not a JSON object")

        return scheme_code, data, None

    except (
        requests.RequestException,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        return scheme_code, None, str(exc)

    except Exception as exc:
        return (
            scheme_code,
            None,
            f"{type(exc).__name__}: {exc}",
        )

    finally:
        session.close()


def process_scheme(scheme_code):
    """
    Fetch and transform one scheme.

    All stock-data sections are consolidated into one stock_data
    DataFrame, with source_type identifying the original API section.

    All mapping sections are consolidated into one stock_mapping
    DataFrame, with source_type identifying the original API section.
    """
    scheme_code, data, error = fetch_scheme(scheme_code)

    if error:
        return scheme_code, None, error

    result = {}

    # --------------------------------------------------------
    # FUND INFO
    # --------------------------------------------------------

    fund_df = records_to_df(data.get("fund_info"))

    if not fund_df.empty:
        fund_df["schemecode"] = scheme_code
        result["fund_info"] = fund_df

    # --------------------------------------------------------
    # CONSOLIDATED STOCK DATA
    # --------------------------------------------------------

    stock_frames = []

    source_names = {
        "stock_data": "stock_data",
        "stock_data_debt": "stock_data_debt",
        "stock_data_cash": "stock_data_cash",
        "stock_data_misc": "stock_data_misc",
    }

    for key, source_type in source_names.items():
        df = latest_entries(data.get(key))

        if not df.empty:
            df["schemecode"] = scheme_code
            df["source_type"] = source_type
            stock_frames.append(df)

    if stock_frames:
        result["stock_data"] = pd.concat(
            stock_frames,
            ignore_index=True,
        )

    # --------------------------------------------------------
    # CONSOLIDATED STOCK MAPPING
    # --------------------------------------------------------

    mapping_frames = []

    mapping_sources = {
        "stock_mapping": "stock_mapping",
        "stock_mapping_debt": "stock_mapping_debt",
        "stock_mapping_cash": "stock_mapping_cash",
        "stock_mapping_misc": "stock_mapping_misc",
    }

    for key, source_type in mapping_sources.items():
        df = mapping_df(
            data.get(key),
            scheme_code,
        )

        if not df.empty:
            df["source_type"] = source_type
            mapping_frames.append(df)

    if mapping_frames:
        result["stock_mapping"] = pd.concat(
            mapping_frames,
            ignore_index=True,
        )

    return scheme_code, result, None


# ============================================================
# MAIN PROCESS
# ============================================================

def load_scheme_codes():
    """Load unique scheme codes from MFCodes.csv."""
    df = pd.read_csv(
        MF_CODES_FILE,
        usecols=["schemecode"],
    )

    return (
        df["schemecode"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )


def save_results(results):
    """Save all collected DataFrames to CSV."""
    for key, filename in OUTPUT_FILES.items():
        frames = results.get(key, [])

        if frames:
            output_df = pd.concat(
                frames,
                ignore_index=True,
            )
        else:
            output_df = pd.DataFrame()

        output_df.to_csv(
            filename,
            index=False,
            encoding="utf-8-sig",
        )


def main():
    scheme_codes = load_scheme_codes()

    total = len(scheme_codes)

    print(
        f"Processing {total:,} scheme codes "
        f"with {MAX_WORKERS} workers..."
    )

    logging.info(
        "Started processing %s scheme codes",
        total,
    )

    results = {
        key: []
        for key in DATA_KEYS
    }

    failed = []

    # --------------------------------------------------------
    # FIRST PASS
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_scheme,
                scheme_code,
            ): scheme_code
            for scheme_code in scheme_codes
        }

        for completed, future in enumerate(
            as_completed(futures),
            1,
        ):
            scheme_code = futures[future]

            try:
                code, result, error = future.result()

                if error:
                    failed.append(code)

                    logging.error(
                        "Failed scheme %s | %s",
                        code,
                        error,
                    )

                else:
                    for key, df in result.items():
                        if (
                            isinstance(df, pd.DataFrame)
                            and not df.empty
                        ):
                            results[key].append(df)

            except Exception as exc:
                failed.append(scheme_code)

                logging.exception(
                    "Unexpected error for scheme %s",
                    scheme_code,
                )

            success_count = completed - len(failed)

            print(
                f"\rProcessed {completed:,}/{total:,} | "
                f"Success: {success_count:,} | "
                f"Failed: {len(failed):,}",
                end="",
            )

    print()

    # --------------------------------------------------------
    # RETRY FAILED SCHEME CODES
    # --------------------------------------------------------

    if failed:
        print(
            f"\nRetrying {len(failed):,} failed scheme codes..."
        )

        logging.info(
            "Retrying %s failed scheme codes",
            len(failed),
        )

    retry_failed = []

    for retry_number, scheme_code in enumerate(
        failed,
        1,
    ):
        delay = random.uniform(
            MIN_FAILED_RETRY_DELAY,
            MAX_FAILED_RETRY_DELAY,
        )

        print(
            f"Retry {retry_number}/{len(failed)} | "
            f"Scheme {scheme_code} | "
            f"Waiting {delay:.1f}s..."
        )

        time.sleep(delay)

        code, result, error = process_scheme(
            scheme_code
        )

        if error:
            retry_failed.append(code)

            logging.error(
                "Retry failed for scheme %s | %s",
                code,
                error,
            )

            print(
                f"Retry failed: {code}"
            )

        else:
            for key, df in result.items():
                if (
                    isinstance(df, pd.DataFrame)
                    and not df.empty
                ):
                    results[key].append(df)

            print(
                f"Retry successful: {code}"
            )

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    save_results(results)

    success_count = total - len(retry_failed)

    logging.info(
        "Completed. Success=%s Failed=%s",
        success_count,
        len(retry_failed),
    )

    print()
    print("=" * 60)
    print("COMPLETED")
    print("=" * 60)
    print(f"Total scheme codes : {total:,}")
    print(f"Successful         : {success_count:,}")
    print(f"Failed             : {len(retry_failed):,}")

    if retry_failed:
        print("\nFinal failed scheme codes:")
        print(", ".join(map(str, retry_failed)))

        logging.error(
            "Final failed scheme codes: %s",
            retry_failed,
        )

    print("\nOutput files:")
    for filename in OUTPUT_FILES.values():
        print(f"  - {filename}")

    print("\nStock data source_type values:")
    print("  - stock_data")
    print("  - stock_data_debt")
    print("  - stock_data_cash")
    print("  - stock_data_misc")

    print("\nStock mapping source_type values:")
    print("  - stock_mapping")
    print("  - stock_mapping_debt")
    print("  - stock_mapping_cash")
    print("  - stock_mapping_misc")


if __name__ == "__main__":
    main()
