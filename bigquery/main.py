from google.cloud import bigquery
import os
from pathlib import Path

# Configuration des chemins  
PROJECT_DIR = Path(os.getcwd()).resolve().parent  
CONFIG_DIR = PROJECT_DIR / 'config'  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CONFIG_DIR, 'cricket_data_project_service_key.json')


# Configurer les identifiants du projet
PROJECT_ID = "cricket-stats-etl-gcp"
DATASET_ID = "cricket_dataset"
TABLE_ID = "your_table_name"  # Changez en fonction de votre table

def read_data_from_bigquery():
    """Lit des données depuis une table BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    # Construire la requête SQL
    query = f"""
        SELECT * 
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        LIMIT 10
    """

    # Exécuter la requête
    query_job = client.query(query)
    results = query_job.result()

    # Afficher les résultats
    for row in results:
        print(dict(row))

def delete_table_from_bigquery():
    """Supprime une table BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    # Construire l'identifiant complet de la table
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Supprimer la table
    try:
        client.delete_table(table_ref)
        print(f"La table {table_ref} a été supprimée.")
    except Exception as e:
        print(f"Erreur lors de la suppression de la table : {e}")

if __name__ == "__main__":
    # Exemple : Lire des données
    print("Lecture des données...")
    read_data_from_bigquery()

    # Exemple : Supprimer une table
    print("\nSuppression de la table...")
    delete_table_from_bigquery()
