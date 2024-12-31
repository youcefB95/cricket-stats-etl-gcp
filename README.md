# 🏏 Cricket Statistics Pipeline with Google Cloud Services

## 🌟 Description

Welcome to our cricket statistics project that collects and processes data on **cricket matches** using Google Cloud services. The collected data is integrated into a **BigQuery** database via **Apache Airflow**, and we visualize it using **Looker Studio** to extract valuable insights.

## 🏗️ Project Architecture

![Project Architecture](images/etl-architecture.png)

## ⚙️ Technologies Used

- **Python**: For data retrieval from the Cricbuzz API.
- **Apache Airflow**: For orchestrating data collection and processing.
- **Google Cloud Storage (GCS)**: For storing data files.
- **Google BigQuery**: For data storage and analysis.
- **Looker Studio**: For data visualization.

## 📊 Data Modeling

The data collected from the Cricbuzz API is structured into a schema suitable for analysis. Here’s a brief overview of the data model:

- **Matches Table**:

  - **match_id** (Primary Key): Unique identifier for each match.
  - **team1**: The first team playing in the match.
  - **team2**: The second team playing in the match.
  - **date**: Date of the match.
  - **venue**: Venue where the match is held.
  - **score**: Final scores of both teams.
  - **result**: Outcome of the match.

Insert your data modeling diagram below (if you have one):  
![Data Model](images/data_model.png)

## 🚀 Installation and Launch

Follow the steps below to get this project up and running:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/cricket-statistics-pipeline.git
   ```

   - Créer un bucket dans GCS => bkt-ranking-data-ycb
   - Activer l'API de GCS
   - Créer un compte de service pour ce projet => crickets-project-account

### Attribuer des rôles au compte de service ⇒ crickets-project-account

Pour que le compte de service ait la permission d'interagir avec Google Cloud Storage, vous devez lui attribuer les rôles appropriés.

Lors de la création du compte de service, dans la section "Accorder des rôles à ce compte de service", sélectionnez un rôle. Pour le stockage, vous pouvez utiliser un des rôles suivants : - **Storage Object Admin** : Pour avoir un contrôle complet sur les objets dans les buckets.

- Une fois le compte de service créé, vous serez redirigé vers la page du compte de service. Cliquez sur Générer une nouvelle clé (JSON)

Si ce n'est pas fait lors de la création, allez dans la section IAM

## 📈 Data Collection

Data collection is performed through a Python script that scrapes cricket statistics from the Cricbuzz API.

Code complet Python : Load data from API + Push to GCS => api_data_to_gcs.py

👉 Cricbuzz API Documentation

## Cloud Function => Dataflow => Cloud Composer

1/ Composer

- Créer un cloud composer environnement ⇒ crickets-project-composer-env (europe-west1)

  Composer : Chargement des fichiers pour Airflow
  Ajouter un dossier scripts (avec api_data_to_gcs.py) et le fichier dag.py

- Cloud function ⇒ Créer une fonction trigger_df_job + Activer Cloud functions API
  déclencheur de type Cloud Storage et évenement google.cloud.storage.object.v1.finalized
  on choisit le bucket source qui nous intéresse => bkt-ranking-data-ycb

  ### Rôles nécessaires pour le compte de service pour utiliser cloud function

- roles/artifactregistry.createOnPushWriter
- roles/logging.logWriter
- roles/pubsub.publisher
- roles/cloudbuild.builds.builder on cricket-stats-etl-gcp => pour déployer la function

  Ensuite, on ajoute le code dans main.py et la lib google-api-python-client dans requirements.txt

## 📊 Looker Studio Dashboard

Once the data is collected and processed, you can explore our interactive dashboard built with Looker Studio. The dashboard provides insights and visualizations of the cricket match data, including:

Match Trends: Visual representation of match outcomes over time.
Team Performance: Analysis of individual team performance metrics.
Score Insights: Overview of score distributions among matches.
You can access the dashboard through the Looker Studio interface once the data is loaded.

Insert your Looker Studio image below (if you have one):
Looker

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## 👤 Authors

Your Name
Additional contributors or inspirations can be listed here.

## 📝 License

This project is licensed under the MIT License. For more details, please refer to the LICENSE file.
