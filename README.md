# 🏏 Cricket Statistics Pipeline with Google Cloud Services

Welcome to the exciting world of data engineering! This guide takes you through the intricate steps of building a comprehensive cricket statistics pipeline using Google Cloud services. From fetching data via the Cricbuzz API to creating an interactive Looker Studio dashboard, each phase contributes to a seamless data flow for analysis and visualization.

## 📊 Architecture

![Architecture](https://github.com/vishal-bulbule/cricket-stat-data-engineering-project/blob/master/Architecture.png)

## 🌐 Data Retrieval with Python and Cricbuzz API

We kick off our project by leveraging Python’s capabilities to interface with APIs. In this section, we will explore how to efficiently fetch cricket statistics from the Cricbuzz API and gather the required data.

## ☁️ Storing Data in Google Cloud Storage (GCS)

Once we have our data, the next step is to securely store it in the cloud. We will dive into how to save this data as CSV files in Google Cloud Storage (GCS), ensuring it's accessible and scalable for future processing.

## ⚡ Creating a Cloud Function Trigger

With our data safely stored, we will set up a Cloud Function that acts as the catalyst for our pipeline. This function will trigger upon file upload in the GCS bucket, initiating the next steps of our data processing journey.

## 🔧 Execution of the Cloud Function

Inside the Cloud Function, we’ll carefully craft the code to trigger a Dataflow job. We’ll handle all the necessary parameters to ensure a smooth initiation of the Dataflow job, guiding the flow of our data processing.

## 🛠️ Dataflow Job for BigQuery

The heart of our pipeline is the Dataflow job. Triggered by the Cloud Function, this job manages the transfer of data from the CSV files in GCS to BigQuery. We’ll meticulously configure job settings for optimal performance and accurate data ingestion into BigQuery.

## 📈 Looker Dashboard Creation

Finally, we'll harness the power of BigQuery as a data source for Looker Studio. After configuring Looker to connect with BigQuery, we will create a visually compelling dashboard. This dashboard will serve as the visualization hub, enabling insightful analysis based on data loaded from our cricket statistics pipeline.

![Looker](https://github.com/vishal-bulbule/cricket-stat-data-engineering-project/blob/master/Looker.png)
