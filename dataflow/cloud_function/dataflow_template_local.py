import configparser  
from io import StringIO  
import pandas as pd  
import apache_beam as beam  
from apache_beam.options.pipeline_options import PipelineOptions  
from apache_beam.io import WriteToBigQuery  
import sys  
from google.cloud import bigquery  
import logging  
import os  
from pathlib import Path  

# Configuration du logger  
log_file = "dataflow_local_job.log"  
logging.basicConfig(  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',  
    handlers=[  
        logging.StreamHandler(),  
        logging.FileHandler(log_file)   
    ]  
)  

# Configuration des chemins  
PROJECT_DIR = Path(os.getcwd()).resolve().parent.parent  
CONFIG_DIR = PROJECT_DIR / 'config'  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CONFIG_DIR, 'cricket_data_project_service_key.json')  

def get_existing_dates(output_date_dim):  
    client = bigquery.Client()  
    query = f'SELECT date FROM `{output_date_dim}`'  
    
    try:  
        query_job = client.query(query)  
        results = query_job.result()  
        return [row.date for row in results]  
    except Exception as e:  
        logging.error(f"Erreur lors de la récupération des dates existantes : {e}")  
        return []  

class ProcessCSV(beam.DoFn):  
    def __init__(self, existing_dates):  
        self.existing_dates = existing_dates  

    def process(self, element):  
        # Conversion de la ligne CSV en DataFrame  
        column_names = ["id", "rank", "name", "country", "rating", "points", "lastUpdatedOn", "trend", "faceImageId", "countryId", "difference"]  
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
        player_dim = player_dim[['player_id', 'first_name', 'last_name']].drop_duplicates()  

        country_dim = data[["countryId", "country"]].drop_duplicates().copy()  
        country_dim.rename(columns={"countryId": "country_id"}, inplace=True)  

        # Conversion de lastUpdatedOn en datetime compatible avec BigQuery  
        data['lastUpdatedOn'] = pd.to_datetime(data['lastUpdatedOn'], errors='coerce')  

        date_dim = pd.DataFrame({  
            'date': data['lastUpdatedOn'].drop_duplicates()  
        })  
        # Conversion explicite des dates vers un format compatible avec BigQuery  
        #date_dim['date'] = date_dim['date'].dt.to_pydatetime()  
        # Formatage pour BigQuery (au format chaîne 'YYYY-MM-DD')  
         
        date_dim['date_id'] = range(1, len(date_dim) + 1)  
        date_dim['day_of_week'] = date_dim['date'].dt.day_name()  
        date_dim['month'] = date_dim['date'].dt.month  
        date_dim['year'] = date_dim['date'].dt.year  
        date_dim['date'] = date_dim['date'].dt.strftime('%Y-%m-%d')  # Formatage pour BigQuery
        date_dim = date_dim[["date_id", "date", "day_of_week", "month", "year"]]

        # Filtrer les dates qui n'existent pas déjà  
        new_dates = date_dim[~date_dim['date'].isin(self.existing_dates)].drop_duplicates(subset=['date'])  

        if new_dates.empty:  
            logging.info("Aucune nouvelle date à insérer, aucune donnée ne sera retournée.")  
            return  

        rankings = data[['id', 'countryId', 'rank', 'rating', 'points', 'difference', 'lastUpdatedOn', 'trend']].copy()  
        rankings.rename(columns={'id': 'player_id', 'countryId': 'country_id'}, inplace=True)  
        
        # Assurez-vous que lastUpdatedOn est de type date 
        rankings['lastUpdatedOn'] = rankings['lastUpdatedOn'].dt.strftime('%Y-%m-%d') 

        rankings = rankings.merge(new_dates, left_on='lastUpdatedOn', right_on='date', how='left')  
        rankings = rankings[["player_id", "country_id", "date_id", "rank", "rating", "points", "difference", "trend"]]  
        rankings.drop_duplicates(inplace=True)
        
        logging.info("date_dim shape is : {}".format(date_dim.shape))  

        # Écrire dans des dictionnaires pour BigQuery  
        for r in player_dim.to_dict(orient='records'):  
            yield {'type': 'player', 'data': r}  
        for r in country_dim.to_dict(orient='records'):  
            yield {'type': 'country', 'data': r}  
        for r in new_dates.to_dict(orient='records'):  
            yield {'type': 'date', 'data': r}  
        for r in rankings.to_dict(orient='records'):  
            yield {'type': 'ranking', 'data': r}  

def filter_header(line):  
    return not line.startswith('id,')  

def run(config_file='config.ini'):  
    config = configparser.ConfigParser()  
    config.read(config_file)  

    project = config['DEFAULT']['project']  
    region = config['DEFAULT']['region']  
    runner = config['DEFAULT']['runner']  
    staging_location = config['DEFAULT']['staging_location']  
    temp_location = config['DEFAULT']['temp_location']  
    input_file = config['DEFAULT']['input']  
    output_player_dim = config['DEFAULT']['output_player_dim']  
    output_country_dim = config['DEFAULT']['output_country_dim']  
    output_date_dim = config['DEFAULT']['output_date_dim']  
    output_rankings = config['DEFAULT']['output_rankings']  
    save_main_session = config.getboolean('DEFAULT', 'save_main_session')  

    existing_dates = get_existing_dates(output_date_dim)  

    options = PipelineOptions(  
        runner=runner,  
        project=project,  
        temp_location=temp_location,  
        staging_location=staging_location,  
        region=region,  
        save_main_session=save_main_session  
    )  

    with beam.Pipeline(options=options) as pipeline:  
        rows = (  
            pipeline  
            | 'ReadFromCSV' >> beam.io.ReadFromText(input_file)  
            | 'FilterHeader' >> beam.Filter(filter_header)  
            | 'ProcessCSV' >> beam.ParDo(ProcessCSV(existing_dates))  
        )  

        (  
            rows  
            | 'FilterPlayer' >> beam.Filter(lambda x: x['type'] == 'player')  
            | 'ExtractPlayerData' >> beam.Map(lambda x: x['data'])  
            | 'WritePlayerToBQ' >> WriteToBigQuery(  
                output_player_dim,  
                schema='player_id:INTEGER,first_name:STRING,last_name:STRING',  
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
            )  
        )  

        (  
            rows  
            | 'FilterCountry' >> beam.Filter(lambda x: x['type'] == 'country')  
            | 'ExtractCountryData' >> beam.Map(lambda x: x['data'])  
            | 'WriteCountryToBQ' >> WriteToBigQuery(  
                output_country_dim,  
                schema='country_id:INTEGER,country:STRING',  
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
            )  
        )  

        (  
            rows  
            | 'FilterDate' >> beam.Filter(lambda x: x['type'] == 'date')  
            | 'ExtractDateData' >> beam.Map(lambda x: x['data'])  
            | 'ConvertToTuple' >> beam.Map(lambda x: (x['date_id'], x['date'], x['day_of_week'], x['month'], x['year']))  # Convertir en tuple  
            | 'DeduplicateDates' >> beam.Distinct()   
            | 'ConvertBackToDict' >> beam.Map(lambda x: {  
                    'date_id': x[0],  
                    'date': x[1],  
                    'day_of_week': x[2],  
                    'month': x[3],  
                    'year': x[4]  
                })  
            | 'WriteDateToBQ' >> WriteToBigQuery(  
                output_date_dim,  
                schema='date_id:INTEGER,date:DATE,day_of_week:STRING,month:INTEGER,year:INTEGER',  
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
            )  
        )  

        (  
            rows  
            | 'FilterRankings' >> beam.Filter(lambda x: x['type'] == 'ranking')  
            | 'ExtractRankingsData' >> beam.Map(lambda x: x['data'])  
            | 'WriteRankingsToBQ' >> WriteToBigQuery(  
                output_rankings,  
                schema='player_id:INTEGER,country_id:INTEGER,date_id:INTEGER,rank:INTEGER,rating:INTEGER,points:INTEGER,difference:FLOAT,trend:STRING',  
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  
            )  
        )  

if __name__ == '__main__':  
    # Vérifier si un argument a été passé  
    if len(sys.argv) > 1:  
        run(config_file=sys.argv[1])  # Utiliser l'argument passé  
    else:  
        run()  # Utiliser le fichier de configuration par défaut