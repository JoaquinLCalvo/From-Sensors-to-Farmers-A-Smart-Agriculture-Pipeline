# From Sensors to Farmers
## A Smart Agriculture End-to-End Pipeline

#### Motivation

Farming decisions often rely on intermediaries to interpret complex data from IoT sensors, leading to inefficiencies and delays. This project addresses the challenge by building a distributed system that processes IoT sensor data and weather information, transforming it into actionable insights. The focus lies on leveraging big data technologies for efficient ETL processes, optimized storage, and data modeling, with PowerBI serving as the intuitive front-end for farmers to access the insights from their phones.


#### Project Overview

This system integrates IoT sensor data, including soil moisture, temperature, and humidity, with weather data from external APIs. Built over a distributed architecture, it combines real-time data processing and batch analytics to deliver actionable insights. The pipeline supports scalability, low-latency processing, and adaptability, making it suitable for a variety of smart agriculture use cases.

##### Data sources:

- **Soil Sensor Data**: Historical data sourced from Melbourne’s agriculture department.
- **Weather Data**: Live updates retrieved via Open Meteo APIs.
 
While developed as a prototype for a Big Data course, the project highlights the practical applications of distributed ETL technologies in precision farming.


#### Technologies

| **Technology**    | **Role**                                                                                 | **Why**                                                                                     |
|--------------------|-----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| **API (Open Meteo)** | Fetches real-time weather data to enhance IoT sensor insights.                          | Open Meteo provides accurate, localized weather data with real-time updates, ideal for precision farming. |
| **Parquet**        | Stores sensor data in an efficient, compact columnar format.                            | Parquet ensures high compression and fast querying compared to CSV, reducing storage costs and improving performance. |
| **Apache Beam**    | Manages real-time data streaming and ETL tasks over a distributed system.               | Apache Beam’s platform-independent model enables seamless processing across large-scale data streams. |
| **MQTT**           | Facilitates lightweight communication between IoT devices and the data pipeline.        | MQTT’s minimal bandwidth requirements make it optimal for remote, low-latency environments. |
| **Apache Spark**   | Performs batch processing and analytics on large datasets.                              | Spark’s distributed framework allows fast and scalable analysis of IoT time-series data. |
| **DuckDB**         | Executes SQL queries on processed data with an in-memory engine.                        | DuckDB’s lightweight structure is better suited for this prototype than heavier relational databases. |
| **PowerBI**        | Displays real-time dashboards accessible via mobile devices.                            | PowerBI’s user-friendly interface makes it accessible to farmers without technical expertise. |
| **FastAPI**        | Exposes the processed data through APIs for integration and external use.               | FastAPI’s asynchronous capabilities ensure efficient handling of simultaneous API requests. |




#### Functionalities

 Soil and Weather Data Collection
 Real-time Data Processing with Apache Beam
 Data Ingestion via MQTT Broker
 Batch Data Processing with Apache Spark
 Storage and Retrieval in DuckDB
 Machine Learning Model Training and Predictions
 Visualization with PowerBI Dashboards
 Real-time Alerts and Notifications for Farmers
 Advanced Data Analytics for Predictive Insights (Future scope)
 Integration with Additional IoT Sensors (Future scope)


##### Pipeline Demonstration

**To run the pipeline:**
1. Inside the Backend folder:
	- `python build.py`: Builds the containers and installs required Python packages.
	- `python run.py`: Runs the pipeline.
	- `python stop.py`: Stops and removes the containers.

**To run the API server:**
2. Inside the Frontend folder:
	- `python run_api_server.py`: Builds the FastAPI container and starts the server.

**To visualize the dashboard:**
- Download and open the PowerBI file in PowerBI Desktop.
- [Demo video of the dashboard](https://drive.google.com/file/u/0/d/1kjKF9z_bRc3xRIDY-XVKt0MbEzw-mFZ2/view?usp=sharing&pli=1).

