import requests  
import configparser  
import os  
from pathlib import Path  
import pandas as pd  
from google.cloud import storage  
import json


# Constants  
DAGS_DIR = Path(__file__).resolve().parent.parent  
CONF_DIR = DAGS_DIR / 'config'  
RAPIDAPI_CONF = CONF_DIR / 'rapid-api.conf'  
GCS_CONF = CONF_DIR / 'gcs.conf'  
CSV_FILENAME = 'batsmen_rankings.csv'  


def api_data_to_gcs():  
    # Charger les fichiers de configuration  
    config = configparser.ConfigParser()  
    


    try:  
        config.read(RAPIDAPI_CONF)  # Lire le fichier de configuration RapidAPI  
        config.read(GCS_CONF)        # Lire le fichier de configuration GCS  
    except Exception as e:  
        print(f"Error reading config files: {e}")  
        return  

    X_RapidAPI_Key = config['RAPIDAPI']['X-RapidAPI-Key']# Ajoutez cette ligne pour obtenir la valeur de X-RapidAPI-Host

    url = "https://crickbuzz-official-apis.p.rapidapi.com/rankings/batsman/"

    querystring = {"formatType":"odi","men":"1"}

    headers = {
        "x-rapidapi-key": X_RapidAPI_Key,
        "x-rapidapi-host": "crickbuzz-official-apis.p.rapidapi.com"
    }


    try:  
        response = requests.get(url, headers=headers, params=querystring)  
        response.raise_for_status()  # Vérifier les erreurs HTTP  
    except requests.exceptions.RequestException as e:  
        print(f"Failed to fetch data: {e}")  
        return  
    
    data = response.json().get('rank', [])  

    
    if data:  
        # Convertir les données en DataFrame et les écrire dans un fichier CSV  
        df = pd.DataFrame(data)  
        
        # Sauvegarder les données dans un fichier CSV  
        csv_path = DAGS_DIR / CSV_FILENAME  
        df.to_csv(csv_path, index=False)  
        print(f"Data fetched successfully and written to '{CSV_FILENAME}'")  
        
        # Upload the CSV file to GCS  
        try:  
            bucket_name = config.get('GCS', 'BUCKET')  
            storage_client = storage.Client()  
            bucket = storage_client.bucket(bucket_name)  
            destination_blob_name = f'{CSV_FILENAME}'  # The path to store in GCS  

            blob = bucket.blob(destination_blob_name)  
            blob.upload_from_filename(csv_path)  
            print(f"File '{CSV_FILENAME}' uploaded to GCS bucket '{bucket_name}' as '{destination_blob_name}'")  
        except Exception as e:  
            print(f"Failed to upload file to GCS: {e}")  
    else:  
        print("No data available from the API.")  


