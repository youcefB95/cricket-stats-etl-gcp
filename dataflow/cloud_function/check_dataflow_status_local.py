import logging
import json
from google.cloud import firestore
from google.cloud import bigquery
import datetime

# Initialisation du client Firestore
db = firestore.Client()

# Initialisation du client BigQuery
bq_client = bigquery.Client()

# Configuration du logger pour les logs locaux dans le fichier 'mylog.log'
log_file = "check_dataflow_status.log"
logging.basicConfig(
    level=logging.INFO,  # Niveau de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format du log
    handlers=[
        logging.StreamHandler(),  # Affiche aussi les logs dans la console
        logging.FileHandler(log_file)  # Écrit les logs dans 'mylog.log'
    ]
)

import os
from pathlib import Path

# Configuration des chemins  
PROJECT_DIR = Path(os.getcwd()).resolve().parent.parent  
CONFIG_DIR = PROJECT_DIR / 'config'  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CONFIG_DIR, 'cricket_data_project_service_key.json')
import google.auth

# Fonction simulée de traitement du message Pub/Sub
def check_dataflow_status(event, context):  
  
    # Obtention des identifiants (credentials):
    credentials, _ = google.auth.default()
    """Cette fonction vérifie le statut du job Dataflow et l'enregistre dans Firestore."""  
    # Décodage du message Pub/Sub
    pubsub_message = json.loads(event['data'])  

    # Récupération des informations du message
    status = pubsub_message.get('status')
    timestamp = pubsub_message.get('timestamp')

    # Enregistrement du statut dans Firestore
    doc_ref = db.collection('dataflow_status').add({
        'status': status,
        'timestamp': timestamp
    })  

    logging.info(f"Message enregistré dans Firestore avec ID: {doc_ref.id}")

    # Vérification du dernier statut enregistré dans Firestore
    last_status = get_last_dataflow_status()

    if last_status:
        logging.info(f"Le dernier job Dataflow a le statut: {last_status['status']}.")
        
        # Vérification si le dernier statut est "success"
        if last_status['status'] == 'success':
            logging.info("Le dernier job Dataflow a réussi. Création de la table analytics dans BigQuery.")
            create_table_in_bigquery()  # Appeler la fonction pour créer une table dans BigQuery
        else:
            logging.warning("Le dernier job Dataflow n'a pas réussi.")
    else:
        logging.warning("Aucun statut disponible dans Firestore.")

def get_last_dataflow_status():  
    """Récupère le dernier statut du job Dataflow enregistré dans Firestore."""  
    # Requêter pour obtenir le dernier statut trié par timestamp
    status_query = db.collection('dataflow_status').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1)
    results = status_query.stream()

    for doc in results:
        return doc.to_dict()  # Retourne le dernier document trouvé

    return None  # Retourne None si aucun document n'est trouvé

def create_table_in_bigquery():  
    """Crée une table analytics dans BigQuery en exécutant une requête SQL."""  
    dataset_id = 'cricket_dataset'  # Remplace par l'ID de ton dataset
    table_id = 'analytics'  # Nom de la table analytics
    project_id = 'cricket-stats-etl-gcp'  # Remplace par ton ID de projet

    # Requête SQL pour créer la table analytics
    sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    SELECT
        r.player_id,
        r.country_id,
        r.date_id,
        r.rank,
        r.rating,
        r.points,
        r.difference,
        r.trend,
        pd.first_name,
        pd.last_name,
        cd.country,
        dd.date,
        dd.day_of_week,
        dd.month,
        dd.year
    FROM
        `{project_id}.{dataset_id}.rankings` r
    JOIN
        `{project_id}.{dataset_id}.player_dim` pd
        ON r.player_id = pd.player_id
    JOIN
        `{project_id}.{dataset_id}.country_dim` cd
        ON r.country_id = cd.country_id
    JOIN
        `{project_id}.{dataset_id}.date_dim` dd
        ON r.date_id = dd.date_id;
    """

    try:
        # Exécute la requête SQL
        query_job = bq_client.query(sql)  # Lancer la requête
        query_job.result()  # Attendre que la requête se termine

        logging.info(f"La table {table_id} a été créée ou remplacée avec succès dans le dataset {dataset_id}.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table {table_id}: {str(e)}")


# Simulation d'un message Pub/Sub
def simulate_event():
    # Crée un message simulé qui sera envoyé à la fonction
    event = {
        'data': json.dumps({
            'status': 'success',
            'timestamp': datetime.datetime.utcnow().timestamp()
        })
    }

    # Simuler l'appel de la fonction avec ce message
    check_dataflow_status(event, None)


# Exécuter le test local
if __name__ == "__main__":
    simulate_event()
