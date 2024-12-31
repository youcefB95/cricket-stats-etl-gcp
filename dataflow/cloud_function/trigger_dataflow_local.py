import logging  
import subprocess  
import os  
from google.cloud import storage  
from pathlib import Path

# Configuration du logger  
logging.basicConfig(level=logging.INFO)  

# Configuration du logger  
log_file = "cloud_function.log"
logging.basicConfig(
    level=logging.INFO,  # Niveau de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format du log
    handlers=[
        logging.StreamHandler(),  # Affiche aussi les logs dans la console
        logging.FileHandler(log_file)  # Écrit les logs dans 'mylog.log'
    ]
)

# Configuration des chemins  
BUCKET_NAME = "cricket_stats_dataflow"  # Remplacez par votre nom de bucket  
CONFIG_FILE_PATH = "config/config.ini"  # Chemin local à partir duquel lire la configuration  


PROJECT_DIR = Path(os.getcwd()).resolve().parent.parent
CONFIG_DIR = PROJECT_DIR / 'config' 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CONFIG_DIR / 'cricket_data_project_service_key.json')


# Données simulées  
data = {  
    "name": "batsmen_rankings.csv",  
    "bucket": "bkt-ranking-data-ycb",  
    # Ajoutez d'autres propriétés si nécessaire  
}  

context = {}  # Vous pouvez ajouter des informations contextuelles si nécessaire  



def download_file(bucket_name, file_name, local_path):  
    """Télécharge un fichier depuis GCS et le stocke localement."""  
    client = storage.Client()  
    bucket = client.bucket(bucket_name)  
    blob = bucket.blob(file_name)  
    
    # Créer le répertoire de destination si nécessaire  
    os.makedirs(os.path.dirname(local_path), exist_ok=True)  

    blob.download_to_filename(local_path)  
    return local_path  

def trigger_dataflow(data, context):  
    """Fonction qui déclenche le job Dataflow à partir d'un script Python."""  
    file_name = data['name']  
    bucket_name = data['bucket']  

    if file_name == "batsmen_rankings.csv" and bucket_name == "bkt-ranking-data-ycb":  
        logging.info(f"Démarrage du job Dataflow pour le fichier: {file_name}")  

        # Télécharger le script Dataflow et le fichier de configuration  
        script_name = "templates/dataflow_template.py"  # Nom de votre script  
        config_name = "config/config.ini"  # Nom de votre fichier de configuration  
        
        script_path = download_file("cricket_stats_dataflow", script_name, f'/tmp/{script_name}')  
        config_path = download_file("cricket_stats_dataflow", config_name, f'/tmp/{config_name}')  

        # Exécuter le script Dataflow avec le chemin du fichier de configuration  
        try:  
            result = subprocess.run(['python', script_path, config_path], check=True, capture_output=True, text=True)  
            logging.info(f"Job Dataflow exécuté avec succès: {result.stdout}")  
        except subprocess.CalledProcessError as e:  
            logging.error(f"Erreur lors de l'exécution du job Dataflow: {e.stderr}")  

    else:  
        logging.info(f"Fichier ignoré: {file_name} dans le bucket {bucket_name}.")
        
        
trigger_dataflow(data,context)