import pandas as pd

# --------------------------------------------------
# 1. Load the three CSV files
# --------------------------------------------------

stock_data = pd.read_csv("stock_data.csv")
fund_info = pd.read_csv("fund_info.csv")
stock_mapping = pd.read_csv("stock_mapping.csv")

# Remove unwanted unnamed columns
stock_data = stock_data.loc[
    :, ~stock_data.columns.str.startswith("Unnamed:")
]


# --------------------------------------------------
# 2. Create composite key: schemecode + fincode
# --------------------------------------------------

def create_key(df):
    return (
        df["schemecode"].astype("string").str.strip()
        + df["fincode"].astype("string").str.strip()
    )

stock_data["_key"] = create_key(stock_data)
stock_mapping["_key"] = create_key(stock_mapping)


# --------------------------------------------------
# 3. VLOOKUP stock name from stock_mapping
# --------------------------------------------------

stock_mapping_lookup = (
    stock_mapping[["_key", "name"]]
    .drop_duplicates("_key")
)

stock_data = stock_data.merge(
    stock_mapping_lookup,
    on="_key",
    how="left",
    validate="many_to_one"
)

stock_data.rename(
    columns={"name": "stock_name"},
    inplace=True
)

stock_data.drop(columns="_key", inplace=True)


# --------------------------------------------------
# 4. Join fund information using schemecode
# --------------------------------------------------

fund_columns = [
    "schemecode",
    "s_name",
    "aumdate",
    "aumtotal",
    "classification"
]

fund_info_lookup = (
    fund_info[fund_columns]
    .drop_duplicates()
)

stock_data = stock_data.merge(
    fund_info_lookup,
    on="schemecode",
    how="left",
    validate="many_to_one"
)


# --------------------------------------------------
# 5. Display results
# --------------------------------------------------

# print(stock_data.head())

print(f"\nTotal rows: {len(stock_data):,}")

print(
    "Unmatched stock names:",
    stock_data["stock_name"].isna().sum()
)

print(
    "Unmatched fund information:",
    stock_data["s_name"].isna().sum()
)

# --------------------------------------------------
# 6. Create 1 file with the output
# -------------------------------------------------

stock_data.to_csv("final_stock_data.csv", index=False, encoding="utf-8-sig")

print("CSV file saved as final_stock_data.csv")
