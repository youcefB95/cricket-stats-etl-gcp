import argparse
from io import StringIO
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
import logging
import os
from pathlib import Path
import numpy as np  # Importer numpy pour gérer les NaN

# Configuration des chemins  
PROJECT_DIR = Path(os.getcwd()).resolve().parent  
CONFIG_DIR = PROJECT_DIR / 'config'  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CONFIG_DIR, 'cricket_data_project_service_key.json')

# Configuration du logger pour les logs locaux dans le fichier 'mylog.log'
log_file = "dataflow_job.log"
logging.basicConfig(
    level=logging.INFO,  # Niveau de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format du log
    handlers=[
        logging.StreamHandler(),  # Affiche aussi les logs dans la console
        logging.FileHandler(log_file)  # Écrit les logs dans 'mylog.log'
    ]
)

class ProcessCSV(beam.DoFn):
    def process(self, element):
        # Conversion de la ligne CSV en DataFrame
        column_names = ["id", "rank", "name", "country", "rating", "points", "lastUpdatedOn", "trend", "faceImageId", "countryId", "difference"]
        
        # Créez un DataFrame à partir d'une chaîne de texte CSV
        data = pd.read_csv(StringIO(element), header=None, names=column_names)

        # Remplacez les valeurs manquantes par des valeurs par défaut
        data.fillna({
            'difference': 0,
            'trend': 'Flat',
            'faceImageId': '0'
        }, inplace=True)

        # Traitement des données pour chaque table
        player_dim = data[['id', 'name']].copy()
        player_dim['first_name'] = player_dim['name'].apply(lambda x: x.split(" ")[0])
        player_dim['last_name'] = player_dim['name'].apply(lambda x: " ".join(x.split(" ")[1:]))
        player_dim.rename(columns={'id': 'player_id'}, inplace=True)
        player_dim = player_dim[['player_id', 'first_name', 'last_name']]

        country_dim = data[['countryId', 'country']].drop_duplicates().copy()
        country_dim.rename(columns={'countryId': 'country_id'}, inplace=True)
        
        data['lastUpdatedOn'] = pd.to_datetime(data['lastUpdatedOn'], errors='coerce')
        date_dim = pd.DataFrame({
            'date_id': range(1, len(data['lastUpdatedOn'].drop_duplicates()) + 1),
            'date': data['lastUpdatedOn'].drop_duplicates().reset_index(drop=True)
        })
        date_dim['day_of_week'] = date_dim['date'].dt.day_name()
        date_dim['month'] = date_dim['date'].dt.month
        date_dim['year'] = date_dim['date'].dt.year

        rankings = data[['id', 'countryId', 'rank', 'rating', 'points', 'difference', 'lastUpdatedOn','trend']].copy()
        rankings.rename(columns={'id': 'player_id', 'countryId': 'country_id'}, inplace=True)
        rankings = rankings.merge(date_dim, left_on='lastUpdatedOn', right_on='date', how='left')
        rankings.rename(columns={'date_id': 'date_id'}, inplace=True)
        rankings=rankings[["player_id","country_id","date_id","rank","rating","points","difference","trend"]]
        
        # Écrire dans des dictionnaires pour BigQuery  
        for r in player_dim.to_dict(orient='records'):  
            yield {'type': 'player', 'data': r}  
        for r in country_dim.to_dict(orient='records'):  
            yield {'type': 'country', 'data': r}  
        for r in date_dim.to_dict(orient='records'):  
            yield {'type': 'date', 'data': r}  
        for r in rankings.to_dict(orient='records'):  
            yield {'type': 'ranking', 'data': r}  

def filter_header(line):
    return not line.startswith('id,')

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--region', required=True, help='GCP region')
    parser.add_argument('--runner', required=True, help='Dataflow runner')
    parser.add_argument('--staging_location', required=True, help='Staging location in GCS')
    parser.add_argument('--temp_location', required=True, help='Temporary location in GCS')
    
    # Ajout des options personnalisées
    parser.add_argument('--input', required=True, help='Input file path')
    parser.add_argument('--output_player_dim', required=True, help='Output BigQuery table for player dimension')
    parser.add_argument('--output_country_dim', required=True, help='Output BigQuery table for country dimension')
    parser.add_argument('--output_date_dim', required=True, help='Output BigQuery table for date dimension')
    parser.add_argument('--output_rankings', required=True, help='Output BigQuery table for rankings')

    # Option pour save_main_session (utile pour Dataflow avec des sessions locales)
    parser.add_argument('--save_main_session', action='store_true', help='Save the main session')

    # Parse the arguments passed to the script
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Ajoutez les arguments personnalisés à PipelineOptions
    options = PipelineOptions(
        runner=known_args.runner,
        project=known_args.project,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        region=known_args.region,
        save_main_session=known_args.save_main_session  # Inclure save_main_session si spécifié
    )

    logging.info("Pipeline starting...")

    with beam.Pipeline(options=options) as pipeline:
        logging.info("Reading input CSV file...")

        rows = (
            pipeline
            | 'ReadFromCSV' >> beam.io.ReadFromText(known_args.input)
            | 'FilterHeader' >> beam.Filter(filter_header)
            | 'ProcessCSV' >> beam.ParDo(ProcessCSV())
        )

        logging.info("Filtering and writing data to BigQuery for Player dimension...")
        (
             (rows  
         | 'FilterPlayer' >> beam.Filter(lambda x: x['type'] == 'player')  
         | 'ExtractPlayerData' >> beam.Map(lambda x: x['data'])  # Extraction des données player
         | 'WritePlayerToBQ' >> WriteToBigQuery(  
             known_args.output_player_dim,  
             schema='player_id:INTEGER,first_name:STRING,last_name:STRING',  
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
         ))  
        )

        logging.info("Filtering and writing data to BigQuery for Country dimension...")
        (
             (rows  
         | 'FilterCountry' >> beam.Filter(lambda x: x['type'] == 'country')  
         | 'ExtractCountryData' >> beam.Map(lambda x: x['data'])  # Extraction des données country
         | 'WriteCountryToBQ' >> WriteToBigQuery(  
             known_args.output_country_dim,  
             schema='country_id:INTEGER,country:STRING',  
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
         ))  
        )  

        logging.info("Filtering and writing data to BigQuery for Date dimension...")
        (
             (rows  
         | 'FilterDate' >> beam.Filter(lambda x: x['type'] == 'date')  
         | 'ExtractDateData' >> beam.Map(lambda x: x['data'])  # Extraction des données date
         | 'WriteDateToBQ' >> WriteToBigQuery(  
             known_args.output_date_dim,  
             schema='date_id:INTEGER,date:DATETIME,hour:INTEGER,day_of_week:STRING,month:INTEGER,year:INTEGER',  
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
         ))  
        )

        logging.info("Filtering and writing data to BigQuery for Rankings...")
        (
             (rows  
         | 'FilterRankings' >> beam.Filter(lambda x: x['type'] == 'ranking')  
         | 'ExtractRankingsData' >> beam.Map(lambda x: x['data'])  # Extraction des données rankings
         | 'WriteRankingsToBQ' >> WriteToBigQuery(  
             known_args.output_rankings,  
             schema='player_id:INTEGER,country_id:INTEGER,date_id:INTEGER,rank:INTEGER,rating:INTEGER,points:INTEGER,difference:FLOAT,trend:STRING',  
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
         ))  
        )  

    logging.info("Pipeline execution completed.")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
