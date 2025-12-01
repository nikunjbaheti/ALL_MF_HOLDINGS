import requests
import csv

def fetch_json_response(scheme_code):
    url = f"https://www.rupeevest.com/home/get_mf_portfolio_tracker?schemecode={scheme_code}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses
        json_data = response.json()
        return json_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def extract_stock_mapping(json_response):
    stock_mapping = json_response.get("stock_mapping", {})
    return stock_mapping.items()

def read_existing_data(file_path):
    existing_data = set()

    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                stock_number = row.get("Stock Number")
                stock_name = row.get("Stock Name")
                if stock_number and stock_name:
                    existing_data.add((stock_number, stock_name))
    except FileNotFoundError:
        pass  # Ignore if the file doesn't exist
    except Exception as e:
        print(f"Error reading existing data from CSV: {e}")

    return existing_data

def append_to_csv(file_path, data):
    header = ["Stock Number", "Stock Name"]

    try:
        with open(file_path, 'a', newline='') as file:
            writer = csv.writer(file)

            # Write header if the file is empty
            if file.tell() == 0:
                writer.writerow(header)

            # Write data if it doesn't already exist
            for item in data:
                if item not in read_existing_data(file_path):
                    writer.writerow(item)
    except Exception as e:
        print(f"Error appending to CSV: {e}")

def main():
    # Read scheme code from the CSV file
    csv_file_path = "MFCodes.csv"
    stk_csv_file_path = "StkCode.csv"
    scheme_code_column_name = "schemecode"

    try:
        with open(csv_file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                scheme_code = row.get(scheme_code_column_name)
                if scheme_code:
                    json_response = fetch_json_response(scheme_code)
                    if json_response:
                        stock_mapping = extract_stock_mapping(json_response)
                        print(f"Stock mapping for scheme code {scheme_code}:\n{stock_mapping}")
                        append_to_csv(stk_csv_file_path, stock_mapping)
                    else:
                        print(f"Failed to fetch JSON response for scheme code {scheme_code}")
                else:
                    print(f"Scheme code not found in row: {row}")
    except FileNotFoundError:
        print(f"CSV file not found: {csv_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
