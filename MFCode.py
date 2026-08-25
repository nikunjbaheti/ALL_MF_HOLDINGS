import requests
import csv

url = "https://www.rupeevest.com/home/get_search_data"
csv_file_name = "MFCodes.csv"

# Make a GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    json_data = response.json()

    # Extract relevant data from the JSON response
    data_to_write = []
    for item in json_data['search_data']:
        s_name1 = item.get('s_name1', '')
        fund_house = item.get('REPLACE (sr.fund_house,\'-\',\' \')', '')  # Corrected variable name
        schemecode = item.get('schemecode', '')
        alt_name = item.get('s_name', '')  # Corrected field name

        # Replace '-' with ' ' in the fund house name
        fund_house = fund_house.replace('-', ' ')

        # Append the data to the list
        data_to_write.append({
            "s_name1": s_name1,
            "fund_house": fund_house,
            "schemecode": schemecode,
            "s_name": alt_name
        })

    # Write the data to a CSV file
    with open(csv_file_name, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ["s_name1", "fund_house", "schemecode", "s_name"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        for row in data_to_write:
            writer.writerow(row)
