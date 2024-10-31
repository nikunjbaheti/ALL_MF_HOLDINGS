import requests
import pandas as pd

# Load all scheme codes from the third column of MFCodes.csv
mf_codes_df = pd.read_csv("MFCodes.csv")
scheme_codes = mf_codes_df.iloc[:, 2]  # Extract third column as scheme codes list

# Function to add scheme code and save DataFrame to CSV
def save_with_scheme_code(df, filename, scheme_code):
    df.insert(0, "scheme_code", scheme_code)  # Insert scheme code as the first column
    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)

# Function to append data to existing CSV for mapping files
def append_to_mapping_file(df, filename):
    try:
        existing_df = pd.read_csv(filename)
        combined_df = pd.concat([existing_df, df]).drop_duplicates().reset_index(drop=True)
    except FileNotFoundError:
        combined_df = df
    combined_df.to_csv(filename, index=False)

# Loop through each scheme code
for scheme_code in scheme_codes:
    print(f"Processing scheme code: {scheme_code}")
    
    # Fetch data for the current scheme code
    url = f"https://www.rupeevest.com/home/get_mf_portfolio_tracker?schemecode={scheme_code}"
    response = requests.get(url)
    data = response.json()

    # Process and save 'fund_info' data with scheme code
    fund_info_df = pd.DataFrame(data.get("fund_info", []))
    save_with_scheme_code(fund_info_df, "fund_info.csv", scheme_code)

    # Process and save 'stock_data' with scheme code
    stock_data_flat = [item for sublist in data.get("stock_data", []) for item in sublist]
    stock_data_df = pd.DataFrame(stock_data_flat)
    save_with_scheme_code(stock_data_df, "stock_data.csv", scheme_code)

    # Process and save 'stock_data_debt' with scheme code
    stock_data_debt_flat = [item for sublist in data.get("stock_data_debt", []) for item in sublist]
    stock_data_debt_df = pd.DataFrame(stock_data_debt_flat)
    save_with_scheme_code(stock_data_debt_df, "stock_data_debt.csv", scheme_code)

    # Process and save 'stock_data_cash' with scheme code
    stock_data_cash_flat = [item for sublist in data.get("stock_data_cash", []) for item in sublist]
    stock_data_cash_df = pd.DataFrame(stock_data_cash_flat)
    save_with_scheme_code(stock_data_cash_df, "stock_data_cash.csv", scheme_code)

    # Process and save 'stock_data_misc' with scheme code
    stock_data_misc_flat = [item for sublist in data.get("stock_data_misc", []) for item in sublist]
    stock_data_misc_df = pd.DataFrame(stock_data_misc_flat)
    save_with_scheme_code(stock_data_misc_df, "stock_data_misc.csv", scheme_code)

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
    save_with_scheme_code(MonthwiseAUM_df, "MonthwiseAUM.csv", scheme_code)

print("Data for all scheme codes successfully saved to CSV files.")
