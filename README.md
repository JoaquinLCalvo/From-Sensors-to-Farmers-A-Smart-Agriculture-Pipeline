# From-Sensors-to-Farmers-A-Smart-Agriculture-Pipeline

### Abstract
The project was designed for the Big Data Technologies’ course (a.y. 2023/2024).
The purpose of this project creates a smart agriculture monitoring platform that uses IoT sensors and advanced data analytics to improve farm management. It collects data on soil moisture, temperature, and humidity to optimize irrigation and productivity. The data is processed using Apache Beam, MQTT, Apache Spark, and DuckDB, and visualized with PowerBI to give farmers actionable insights.
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
