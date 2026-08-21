import requests
import pandas as pd
import json
import time
from datetime import datetime

# Paths
mf_codes_file = "MFCodes.csv"
log_file_path = "log.txt"

# Initialize accumulators for each part of the JSON response
fund_info_df = pd.DataFrame()
stock_data_df = pd.DataFrame()
stock_mapping_df = pd.DataFrame()
stock_data_debt_df = pd.DataFrame()
stock_mapping_debt_df = pd.DataFrame()
stock_data_cash_df = pd.DataFrame()
stock_mapping_cash_df = pd.DataFrame()
stock_data_misc_df = pd.DataFrame()
stock_mapping_misc_df = pd.DataFrame()
monthwise_aum_df = pd.DataFrame()

# Load scheme codes
mf_codes_df = pd.read_csv(mf_codes_file, usecols=[2], names=['schemecode'], header=0)
schemecodes = mf_codes_df['schemecode'].dropna().unique()

# Logging setup
def log_message(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry + "\n")

def filter_latest_data(df, date_col='invdate'):
    # Ensure 'invdate' is a string before using .str accessor
    if date_col not in df.columns:
        log_message(f"Missing column '{date_col}' in DataFrame. Unable to filter latest data.")
        return df

    # Convert 'invdate' to date format
    df[date_col] = pd.to_datetime(df[date_col].astype(str).str.split("T").str[0], errors='coerce').dt.date
    df = df.dropna(subset=[date_col])

    if not df.empty:
        # Find the maximum date in the 'invdate' column
        max_date = df[date_col].max()
        # Filter rows where 'invdate' is equal to the max date
        latest_entries = df[df[date_col] == max_date]
        return latest_entries.reset_index(drop=True)
    
    return pd.DataFrame()

# Function to process each JSON response
def process_json_response(scheme_code, data):
    global fund_info_df, stock_data_df, stock_mapping_df, stock_data_debt_df
    global stock_mapping_debt_df, stock_data_cash_df, stock_mapping_cash_df
    global stock_data_misc_df, stock_mapping_misc_df

    # Append fund_info data
    if 'fund_info' in data:
        fund_info = pd.DataFrame(data['fund_info'])
        fund_info['schemecode'] = scheme_code
        fund_info_df = pd.concat([fund_info_df, fund_info], ignore_index=True)

    # Append and filter stock_data
    if 'stock_data' in data:
        stock_data_entries = [pd.DataFrame(entry) for entry in data['stock_data']]
        for stock_data in stock_data_entries:
            stock_data['schemecode'] = scheme_code
            stock_data_df = pd.concat([stock_data_df, stock_data], ignore_index=True)

    # Append and filter for latest entries
    stock_data_df = filter_latest_data(stock_data_df)

    # Append stock_mapping data
    if 'stock_mapping' in data:
        stock_mapping = pd.DataFrame(list(data['stock_mapping'].items()), columns=['fincode', 'name'])
        stock_mapping_df = pd.concat([stock_mapping_df, stock_mapping], ignore_index=True)

    # Continue similarly for stock_data_debt, stock_mapping_debt, stock_data_cash, etc.
    # Append and filter stock_data_debt
    if 'stock_data_debt' in data:
        stock_data_debt_entries = [pd.DataFrame(entry) for entry in data['stock_data_debt']]
        for stock_data_debt in stock_data_debt_entries:
            stock_data_debt['schemecode'] = scheme_code
            stock_data_debt_df = pd.concat([stock_data_debt_df, stock_data_debt], ignore_index=True)

    # Append and filter for latest entries in stock_data_debt
    stock_data_debt_df = filter_latest_data(stock_data_debt_df)

    # Append stock_mapping_debt data
    if 'stock_mapping_debt' in data:
        stock_mapping_debt = pd.DataFrame(list(data['stock_mapping_debt'].items()), columns=['fincode', 'name'])
        stock_mapping_debt_df = pd.concat([stock_mapping_debt_df, stock_mapping_debt], ignore_index=True)

    # Append and filter stock_data_cash
    if 'stock_data_cash' in data:
        stock_data_cash_entries = [pd.DataFrame(entry) for entry in data['stock_data_cash']]
        for stock_data_cash in stock_data_cash_entries:
            stock_data_cash['schemecode'] = scheme_code
            stock_data_cash_df = pd.concat([stock_data_cash_df, stock_data_cash], ignore_index=True)

    # Append and filter for latest entries in stock_data_cash
    stock_data_cash_df = filter_latest_data(stock_data_cash_df)

    # Append stock_mapping_cash data
    if 'stock_mapping_cash' in data:
        stock_mapping_cash = pd.DataFrame(list(data['stock_mapping_cash'].items()), columns=['fincode', 'name'])
        stock_mapping_cash_df = pd.concat([stock_mapping_cash_df, stock_mapping_cash], ignore_index=True)

    # Append and filter stock_data_misc
    if 'stock_data_misc' in data:
        stock_data_misc_entries = [pd.DataFrame(entry) for entry in data['stock_data_misc']]
        for stock_data_misc in stock_data_misc_entries:
            stock_data_misc['schemecode'] = scheme_code
            stock_data_misc_df = pd.concat([stock_data_misc_df, stock_data_misc], ignore_index=True)

    # Append and filter for latest entries in stock_data_misc
    stock_data_misc_df = filter_latest_data(stock_data_misc_df)

    # Append stock_mapping_misc data
    if 'stock_mapping_misc' in data:
        stock_mapping_misc = pd.DataFrame(list(data['stock_mapping_misc'].items()), columns=['fincode', 'name'])
        stock_mapping_misc_df = pd.concat([stock_mapping_misc_df, stock_mapping_misc], ignore_index=True)

# Initialize counters
successful_count = 0
unsuccessful_count = 0
unsuccessful_codes = []

# Process each scheme code with a 5-second delay
for index, scheme_code in enumerate(schemecodes):
    try:
        log_message(f"Processing scheme code: {scheme_code}")
        url = f"https://www.rupeevest.com/home/get_mf_portfolio_tracker?schemecode={scheme_code}"
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        process_json_response(scheme_code, data)
        successful_count += 1

    except json.JSONDecodeError:
        log_message(f"Failed to parse JSON for scheme code: {scheme_code}")
        unsuccessful_codes.append(scheme_code)
        unsuccessful_count += 1
    except requests.RequestException as e:
        log_message(f"Request error for scheme code {scheme_code}: {e}")
        unsuccessful_codes.append(scheme_code)
        unsuccessful_count += 1
    except Exception as e:
        log_message(f"Error for scheme code {scheme_code}: {e}")
        unsuccessful_codes.append(scheme_code)
        unsuccessful_count += 1

    time.sleep(2.5)  # Increased delay to 5 seconds

    print(f"Processed {index + 1}/{len(schemecodes)} scheme codes.")
    print(f"Successfully processed {successful_count}/{len(schemecodes)} scheme codes")
    print(f"Unsuccessfully processed {unsuccessful_count}/{len(schemecodes)} scheme codes")

# Retry unsuccessful scheme codes with 5-second delay
for scheme_code in unsuccessful_codes:
    try:
        log_message(f"Retrying scheme code: {scheme_code}")
        url = f"https://www.rupeevest.com/home/get_mf_portfolio_tracker?schemecode={scheme_code}"
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        process_json_response(scheme_code, data)
        successful_count += 1

    except json.JSONDecodeError:
        log_message(f"Failed to parse JSON on retry for scheme code: {scheme_code}")
        unsuccessful_count += 1
    except requests.RequestException as e:
        log_message(f"Request error on retry for scheme code {scheme_code}: {e}")
        unsuccessful_count += 1
    except Exception as e:
        log_message(f"Error on retry for scheme code {scheme_code}: {e}")
        unsuccessful_count += 1

    time.sleep(2.5)  # Increased delay to 5 seconds

    print(f"Successfully processed {successful_count}/{len(schemecodes)} scheme codes")
    print(f"Unsuccessfully processed {unsuccessful_count}/{len(schemecodes)} scheme codes")

# Log final counts and save to CSV
log_message(f"Total successful scheme codes: {successful_count}")
print(f"Total successful scheme codes: {successful_count}")
log_message(f"Total unsuccessful scheme codes: {unsuccessful_count}")
print(f"Total unsuccessful scheme codes: {unsuccessful_count}")

# Save all dataframes to CSV
fund_info_df.to_csv("fund_info.csv", index=False)
stock_data_df.to_csv("stock_data.csv", index=False)
stock_mapping_df.to_csv("stock_mapping.csv", index=False)
stock_data_debt_df.to_csv("stock_data_debt.csv", index=False)
stock_mapping_debt_df.to_csv("stock_mapping_debt.csv", index=False)
stock_data_cash_df.to_csv("stock_data_cash.csv", index=False)
stock_mapping_cash_df.to_csv("stock_mapping_cash.csv", index=False)
stock_data_misc_df.to_csv("stock_data_misc.csv", index=False)
stock_mapping_misc_df.to_csv("stock_mapping_misc.csv", index=False)
