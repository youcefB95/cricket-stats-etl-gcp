from google.cloud import storage
import os
from pathlib import Path


PROJECT_DIR = Path(os.getcwd()).resolve().parent
CONFIG_DIR = PROJECT_DIR / 'config'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CONFIG_DIR / 'cricket_data_project_service_key.json')

def delete_all_files_from_bucket(bucket_name):
    # Crée un client pour accéder à Google Cloud Storage
    client = storage.Client()

    # Récupère le bucket
    bucket = client.get_bucket(bucket_name)

    # Liste tous les objets dans le bucket
    blobs = bucket.list_blobs()

    # Supprime chaque objet
    for blob in blobs:
        print(f"Suppression de {blob.name}...")
        blob.delete()

    print(f"Tous les fichiers ont été supprimés du bucket {bucket_name}.")

# Exemple d'appel
delete_all_files_from_bucket('bkt-ranking-data-yb95')
