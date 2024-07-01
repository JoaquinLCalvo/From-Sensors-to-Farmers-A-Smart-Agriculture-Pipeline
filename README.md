# From-Sensors-to-Farmers-A-Smart-Agriculture-Pipeline

### Abstract
The project was designed for the Big Data Technologies’ course (a.y. 2023/2024).
The purpose of this project is to create a smart agriculture monitoring platform that uses IoT sensors and advanced data analytics to improve farm management. By collecting data on soil moisture, temperature, and humidity from sensors placed Melbourne (https://discover.data.vic.gov.au/dataset/soil-sensor-readings-historical-data), as well as weather data from API (https://open-meteo.com/), the platform optimizes irrigation and productivity, providing farmers with actionable insights.

### Technologies
- Apache Beam
- MQTT
- Apache Spark
- DuckDB
- PowerBI
- FastAPI

### Functionalities
- [x] Soil and Weather Data Collection
- [x] Real-time Data Processing with Apache Beam
- [x] Data Ingestion via MQTT Broker
- [x] Batch Data Processing with Apache Spark
- [x] Storage and Retrieval in DuckDB
- [x] Machine Learning Model Training and Predictions
- [x] Visualization with PowerBI Dashboards
- [x] Real-time Alerts and Notifications for Farmers
- [ ] Advanced Data Analytics for Predictive Insights - Currently limited by the artificial parameters used and the synthetic nature of some data points
- [ ] Integration with Additional IoT Sensors - Potential future expansion as more sensor data becomes available

 ### Architecture
 ### Steps to run the pipeline & display the dashboard

To run the pipeline
Inside Backend : python build.py : to build the containers and install the required python packages
		    python run.py   : to run the containers and start the pipeline
		    python stop.py  : to get down the containers


To run the API server:
   Inside Frontend : python run_api_server.py ; build the fastapi container and start the api server

To run the PowerBI dashboard:
Download PowerBI Desktop and open the file. The hidden tabs are not to be considered as part of the delivery.

To see a demo on video of the dashboard:
Click [here](https://drive.google.com/file/d/1kjKF9z_bRc3xRIDY-XVKt0MbEzw-mFZ2/view?usp=sharing).


To access the API Swagger Documentation
http://localhost:8000/docs
