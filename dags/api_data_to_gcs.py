import requests
import csv
import configparser
import os
from pathlib import Path
import pandas as pd
from google.cloud import storage


DAGS_DIR = Path(os.getcwd()).resolve().parent



def api_data_to_gcs():


    url = "https://crickbuzz-official-apis.p.rapidapi.com/rankings/batsman/"

    querystring = {"formatType":"odi","men":"1"}

    headers = {
        "x-rapidapi-key": "4efebc1457msh533840ee6a8b9e7p1ed720jsne4646468e9fc",
        "x-rapidapi-host": "crickbuzz-official-apis.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json().get('rank', [])  # Extracting the 'rank' data
        
        # Définir le dossier contenant les données
        
        csv_filename = 'batsmen_rankings.csv'
        csv_path = DAGS_DIR / csv_filename

        if data:
            # field_names = ["id", "rank ", "name" , "country", "rating",	"points" ,	
            #                "lastUpdatedOn"  ,"trend","faceImageId","countryId",	"difference"]  # Specify required field names

            # Write data to CSV file with only specified field names
            df = pd.DataFrame(data)
            
            # Sauvegarder les données dans un fichier CSV
            df.to_csv(csv_path, index=False)
            print(f"Data fetched successfully and written to '{csv_filename}'")
            
            # Upload the CSV file to GCS
            bucket_name = 'bkt-ranking-data-yb95'
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            destination_blob_name = f'{csv_filename}'  # The path to store in GCS

            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(csv_path)

            print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")

            
        else:
            print("No data available from the API.")

    else:
        print("Failed to fetch data:", response.status_code)


if __name__ == "__main__":
    api_data_to_gcs()