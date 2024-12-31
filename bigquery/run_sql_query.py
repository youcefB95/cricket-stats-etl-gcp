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
TABLE_ID = "date_dim"  # Changez en fonction de votre table



def execute_parametrized_query(sql_file_path, parameter):
    client = bigquery.Client(project=PROJECT_ID)

    with open(sql_file_path, "r") as file:
        query = file.read()

    # Définir les paramètres
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("parameter", "INT64", parameter)
        ]
    )

    # Exécuter la requête
    #query_job = client.query(query, job_config=job_config)
    query_job = client.query(query)

    results = query_job.result()

    for row in results:
        print(dict(row))



def execute_static_query(sql_file_path):
    """
    Exécute une requête SQL statique sur BigQuery et affiche les résultats.
    """
    # Initialisation du client BigQuery
    client = bigquery.Client(location="europe-west9")

    # Requête SQL statique
    with open(sql_file_path, "r") as file:
        query = file.read()

    try:
        # Exécution de la requête
        query_job = client.query(query)  # Lancement du job
        print("Query executed successfully. Results:")

        
    except Exception as e:
        print(f"An error occurred: {e}")


def execute_dynamic_query_with_params(sql_file_path,params:dict) -> None:
    """
    Exécute une requête SQL après avoir remplacé les placeholders 
    dans le fichier SQL à partir d'un dictionnaire.
    """
    # Initialisation du client BigQuery
    client = bigquery.Client(location="europe-west9")

    # Requête SQL statique
    with open(sql_file_path, "r") as file:
        query = file.read()
        for placeholder, value in params.items():
            query = query.replace(f"{{{placeholder}}}", value)
    try:
        # Exécution de la requête
        query_job = client.query(query)  # Lancement du job
        query_job.result()
        print("Query executed successfully for table " + params["table_name"] + ". Results:")

        
    except Exception as e:
        print(f"An error occurred: {e}")



#execute_parametrized_query("sql/drop_table.sql", 1000)
#execute_static_query("sql/drop_table.sql")


query_params = {
    "project_id":"cricket-stats-etl-gcp",
    "dataset":"cricket_dataset",
    "table_name":"country_dim"}

# execute_dynamic_query_with_params(
#     sql_file_path="sql/default_drop_query.sql",
#     params = query_params
# )

for table in ["player_dim","rankings","country_dim","date_dim","analytics"]: 
       params = query_params
       params["table_name"] = table
       execute_dynamic_query_with_params(
       sql_file_path="sql/default_drop_query.sql",
       params = params
)
       