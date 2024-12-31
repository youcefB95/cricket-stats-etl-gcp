import logging
import json
import base64
from google.cloud import bigquery

# Initialisation du client Firestore
firestore_client = firestore.Client(project="cricket-stats-etl-gcp")

# Initialisation du client BigQuery
bq_client = bigquery.Client()

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Fonction pour traiter les messages Pub/Sub
def check_dataflow_status(event, context):
    """Cette fonction vérifie le statut du job Dataflow et l'enregistre dans Firestore."""
    # Décodage du message Pub/Sub
    pubsub_message = event['data']
    logging.error("Test pubsub_message is : {}".format(pubsub_message))
    logging.error("Test pubsub_message type is : {}".format(type(pubsub_message)))

    # Décoder le message en UTF-8 et le transformer en JSON
    message_data = json.loads(base64.b64decode(pubsub_message).decode('utf-8'))
    logging.error("Test message data is : {}".format(message_data))
    logging.error("Test message data type is : {}".format(type(message_data)))

    # Récupération des informations du message
    status = message_data.get('status')
    timestamp = message_data.get('timestamp')

    if status == 'success':
        logging.info("Le dernier job Dataflow a réussi. Création de la table analytics dans BigQuery.")
        create_table_in_bigquery()  # Appeler la fonction pour créer une table dans BigQuery
    else:
        logging.warning("Le dernier job Dataflow n'a pas réussi.")


def create_table_in_bigquery():
    """Crée une table analytics dans BigQuery en exécutant une requête SQL."""
    dataset_id = 'cricket_dataset'  # Remplace par l'ID de ton dataset
    table_id = 'analytics'  # Nom de la table analytics
    project_id = 'cricket-stats-etl-gcp'  # Remplace par ton ID de projet

    # Requête SQL pour créer la table analytics
    sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    SELECT 
        player_id,
        country_id,
        date_id,
        rank,
        rating,
        points,
        difference,
        trend,
        first_name,
        last_name,
        country,
        date,
        day_of_week,
        month,
        year
    FROM (
        SELECT DISTINCT 
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
            dd.year,
            ROW_NUMBER() OVER (
                PARTITION BY r.player_id, r.date_id
                ORDER BY r.rank DESC
            ) as row_num
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
            ON r.date_id = dd.date_id
    ) 
    WHERE row_num = 1;
    """

    try:
        # Exécute la requête SQL
        query_job = bq_client.query(sql)  # Lancer la requête
        query_job.result()  # Attendre que la requête se termine

        logging.info(f"La table {table_id} a été créée ou remplacée avec succès dans le dataset {dataset_id}.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table {table_id}: {str(e)}")
