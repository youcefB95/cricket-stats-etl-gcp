# Extraire les paramètres de configuration à partir de dataflow-config.json  
GCS_LOCATION=$(jq -r '.GCS_LOCATION' dataflow-config.json)  
PROJECT_ID=$(jq -r '.PROJECT_ID' dataflow-config.json)
STAGING_LOCATION=$(jq -r '.STAGING_LOCATION' dataflow-config.json)
TEMP_LOCATION=$(jq -r '.TEMP_LOCATION' dataflow-config.json)  
INPUT=$(jq -r '.INPUT' dataflow-config.json)  
OUTPUT_PLAYER_DIM=$(jq -r '.OUTPUT.player_dim' dataflow-config.json)  
OUTPUT_COUNTRY_DIM=$(jq -r '.OUTPUT.country_dim' dataflow-config.json)  
OUTPUT_DATE_DIM=$(jq -r '.OUTPUT.date_dim' dataflow-config.json)  
OUTPUT_RANKINGS=$(jq -r '.OUTPUT.rankings' dataflow-config.json)  
JOB_NAME=$(jq -r '.JOB_NAME' dataflow-config.json)  
REGION=$(jq -r '.REGION' dataflow-config.json)  

# Exécuter la commande Dataflow  
python dataflow_template_with_shell.py \
    --project=$PROJECT_ID \
    --region=$REGION \
    --runner=DataflowRunner \
    --staging_location=$STAGING_LOCATION \
    --temp_location=$TEMP_LOCATION \
    --input=$INPUT \
    --output_player_dim=$OUTPUT_PLAYER_DIM \
    --output_country_dim=$OUTPUT_COUNTRY_DIM \
    --output_date_dim=$OUTPUT_DATE_DIM \
    --output_rankings=$OUTPUT_RANKINGS \
    --save_main_session