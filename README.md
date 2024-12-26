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

## 📈 Data Collection

Data collection is performed through a Python script that scrapes cricket statistics from the Cricbuzz API.

👉 Cricbuzz API Documentation

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
