import requests
import pandas as pd

# Load scheme code from MFCodes.csv (third column)
mf_codes_df = pd.read_csv("MFCodes.csv")
scheme_code = mf_codes_df.iloc[0, 2]  # Adjust this index if needed

# URL to fetch data, with dynamic scheme code
url = f"https://www.rupeevest.com/home/get_mf_portfolio_tracker?schemecode={scheme_code}"

# Fetch the data
response = requests.get(url)
data = response.json()

# Function to add scheme code and save DataFrame to CSV
def save_with_scheme_code(df, filename):
    df.insert(0, "scheme_code", scheme_code)  # Insert scheme code as the first column
    df.to_csv(filename, mode='w', index=False)

# Function to append data to existing CSV for mapping files
def append_to_mapping_file(df, filename):
    try:
        existing_df = pd.read_csv(filename)
        combined_df = pd.concat([existing_df, df]).drop_duplicates().reset_index(drop=True)
    except FileNotFoundError:
        combined_df = df
    combined_df.to_csv(filename, index=False)

# Process and save 'fund_info' data with scheme code
fund_info_df = pd.DataFrame(data.get("fund_info", []))
save_with_scheme_code(fund_info_df, "fund_info.csv")

# Process and save 'stock_data' with scheme code
stock_data_flat = [item for sublist in data.get("stock_data", []) for item in sublist]
stock_data_df = pd.DataFrame(stock_data_flat)
save_with_scheme_code(stock_data_df, "stock_data.csv")

# Process and save 'stock_data_debt' with scheme code
stock_data_debt_flat = [item for sublist in data.get("stock_data_debt", []) for item in sublist]
stock_data_debt_df = pd.DataFrame(stock_data_debt_flat)
save_with_scheme_code(stock_data_debt_df, "stock_data_debt.csv")

# Process and save 'stock_data_cash' with scheme code
stock_data_cash_flat = [item for sublist in data.get("stock_data_cash", []) for item in sublist]
stock_data_cash_df = pd.DataFrame(stock_data_cash_flat)
save_with_scheme_code(stock_data_cash_df, "stock_data_cash.csv")

# Process and save 'stock_data_misc' with scheme code
stock_data_misc_flat = [item for sublist in data.get("stock_data_misc", []) for item in sublist]
stock_data_misc_df = pd.DataFrame(stock_data_misc_flat)
save_with_scheme_code(stock_data_misc_df, "stock_data_misc.csv")

# Process and append to 'stock_mapping' CSV
stock_mapping_df = pd.DataFrame(data.get("stock_mapping", {}).items(), columns=["fincode", "name"])
append_to_mapping_file(stock_mapping_df, "stock_mapping.csv")

# Process and append to 'stock_mapping_debt' CSV
stock_mapping_debt_df = pd.DataFrame(data.get("stock_mapping_debt", {}).items(), columns=["fincode", "name"])
append_to_mapping_file(stock_mapping_debt_df, "stock_mapping_debt.csv")

# Process and append to 'stock_mapping_cash' CSV
stock_mapping_cash_df = pd.DataFrame(data.get("stock_mapping_cash", {}).items(), columns=["fincode", "name"])
append_to_mapping_file(stock_mapping_cash_df, "stock_mapping_cash.csv")

# Process and append to 'stock_mapping_misc' CSV
stock_mapping_misc_df = pd.DataFrame(data.get("stock_mapping_misc", {}).items(), columns=["fincode", "name"])
append_to_mapping_file(stock_mapping_misc_df, "stock_mapping_misc.csv")

# Process and save 'MonthwiseAUM' with scheme code
MonthwiseAUM_df = pd.DataFrame(data.get("MonthwiseAUM", []))
save_with_scheme_code(MonthwiseAUM_df, "MonthwiseAUM.csv")

print(f"Data for scheme code {scheme_code} successfully saved to CSV files.")
