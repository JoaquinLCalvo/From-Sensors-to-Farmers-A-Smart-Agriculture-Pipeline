import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import paho.mqtt.publish as publish
from datetime import datetime
import json
import configparser
import csv
from io import StringIO
import logging
import requests

"""
Configuration settings
"""


class Config:
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.mqtt_broker = self.config['mqtt']['broker_address']
        self.mqtt_topic = self.config['mqtt']['topic']
        self.mqtt_username = self.config['mqtt']['username']
        self.mqtt_password = self.config['mqtt']['password']
        self.source_folder = self.config['file_paths']['source_folder']
        self.backup_folder = self.config['file_paths']['backup_folder']
        self.location_latitude = self.config["coordinates"]["latitude"]
        self.location_longitude = self.config["coordinates"]["longitude"]
        self.backup_file = self.config["file_names"]["backup_file"]
        self.source_file = self.config["file_names"]["sensor_data_file"]
        self.time_interval = int(self.config.get(
            'processing_options', 'time_interval', fallback='1'))


class Logger:
    @staticmethod
    def setup():
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)


class Utils:
    @staticmethod
    def convert_to_serializable(element):
        if isinstance(element, dict):
            return {k: Utils.convert_to_serializable(v) for k, v in element.items()}
        elif isinstance(element, list):
            return [Utils.convert_to_serializable(i) for i in element]
        elif isinstance(element, datetime):
            return element.isoformat()
        else:
            return element


class MQTTPublisher:
    def __init__(self, broker, topic, username, password):
        self.broker = broker
        self.topic = topic
        self.username = username
        self.password = password

    def publish(self, element):
        try:
            serializable_element = Utils.convert_to_serializable(element)
            message = json.dumps(serializable_element)
            publish.single(self.topic, payload=message, hostname=self.broker, qos=2, auth={
                           'username': self.username, 'password': self.password})
        except Exception as e:
            logger.error(f"Error publishing to MQTT: {e}")


class FileWriter:
    def __init__(self, output_file):
        self.output_file = output_file

    def write(self, element):
        serializable_element = Utils.convert_to_serializable(element)
        try:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(serializable_element) + '\n')
        except Exception as e:
            logger.error(f"Error writing to text file: {e}")


class WeatherService:
    @staticmethod
    def get_weather_details(date, latitude, longitude):
        try:
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={date}&end_date={date}&daily=weather_code,temperature_2m_mean,precipitation_sum,wind_speed_10m_max,et0_fao_evapotranspiration"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.error(
                    f"Error fetching weather data: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            return None


class PublishAndWrite(beam.DoFn):
    def __init__(self, mqtt_publisher, file_writer, latitude, longitude):
        self.mqtt_publisher = mqtt_publisher
        self.file_writer = file_writer
        self.latitude = latitude
        self.longitude = longitude
        self.weather_cache = {}

    def process(self, element):
        local_time = element["local_time"]
        if not isinstance(local_time, str):
            local_time = str(local_time)

        dt = datetime.fromisoformat(local_time)
        element["date"] = dt.date().isoformat()
        element["time"] = dt.time().isoformat()

        date = element['date']
        if date in self.weather_cache:
            weather = self.weather_cache[date]
        else:
            weather = WeatherService.get_weather_details(
                date, self.latitude, self.longitude)
            self.weather_cache[date] = weather

        combined_data = {'sensor_data': element, 'weather_data': weather}

        self.mqtt_publisher.publish(combined_data)

        """
        Option to write to a backup file
        """
        # self.file_writer.write(combined_data)

        yield


class SensorDataPipeline:
    def __init__(self, config_file):
        self.config = Config(config_file)
        self.logger = Logger.setup()
        self.mqtt_publisher = MQTTPublisher(
            self.config.mqtt_broker, self.config.mqtt_topic, self.config.mqtt_username, self.config.mqtt_password)
        self.file_writer = FileWriter(
            self.config.backup_folder + "/" + self.config.backup_file)

    def run(self):
        options = PipelineOptions()
        with beam.Pipeline(options=options) as p:
            (p
             | 'Read Parquet' >> beam.io.ReadFromParquet(self.config.source_folder + "/" + self.config.source_file)
             | 'Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(self.config.time_interval))
             | 'Publish to MQTT and Write to Text' >> beam.ParDo(PublishAndWrite(self.mqtt_publisher, self.file_writer, self.config.location_latitude, self.config.location_longitude)))


if __name__ == '__main__':
    pipeline = SensorDataPipeline('config.ini')
    pipeline.run()
