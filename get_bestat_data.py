import requests
import pandas as pd

# Send a GET request to the URL
response = requests.get('https://bestat.statbel.fgov.be/bestat/api/views/')

# Ensure we got a successful response
if response.status_code == 200:
    # Parse the response content as JSON
    data_source = response.json()

# create a dataframe from the data
df = pd.DataFrame(data_source)

#select only the rows where locale is 'nl'
df = df[df['locale'] == 'nl'].reset_index(drop=True)
df.to_csv('bestat.csv', index=False)

for index, row in df.iterrows():
        id = row['id']
        download_url = f"https://bestat.statbel.fgov.be/bestat/api/views/{id}/result/CSV"

        try:
            # Send a GET request to the download URL
            response = requests.get(download_url)
            response.raise_for_status()

            try:
                # Read the CSV data
                data = pd.read_csv(download_url)

                # Check if the data is empty
                if data.empty:
                    raise pd.errors.EmptyDataError("Empty CSV file")

                # Save the data to a CSV file
                data.to_csv(f"bestat/{id}.csv", index=False)
                print(f"File saved for id {id}")

            except pd.errors.EmptyDataError:
                print(f"Skipping empty file for id {id}")
                continue  # Skip to the next iteration

        except requests.exceptions.HTTPError as err:
            print(f"Failed to download file for id {id}. HTTP Error: {err}")
            continue  # Skip to the next iteration

        except pd.errors.ParserError as err:
            print(f"Failed to parse CSV for id {id}. Parser Error: {err}")
            continue  # Skip to the next iteration
