from fastapi import FastAPI, Path, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import duckdb
from typing import List, Optional
from datetime import date as Date
from dotenv import load_dotenv
import os

"""
if true, connection will be made to motherduck database, otherwise a local database will be used
"""
USE_REMOTE_DB = False

MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoibWJhcmtpLm1tZWQuZ21haWwuY29tIiwiZW1haWwiOiJtYmFya2kubW1lZEBnbWFpbC5jb20iLCJ1c2VySWQiOiJiMmY4ZWQ4Ni0yMmRmLTQwODktYTUxNi02ZWZmMjQ4ZTBkNmIiLCJpYXQiOjE3MTkyNjc5NzcsImV4cCI6MTc1MDgyNTU3N30.3gsWkj-t49UTkfUgks1BJxgRuGQdJvAyt6qvXxNjnF8"

# Load environment variables from .env
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    # Replace with your frontend URL or * to allow all origins
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Connect to DuckDB file
duckdb_file = 'sensor_data.duckdb'  # Replace with actual path

# Model for response


class SensorData(BaseModel):
    local_time: str
    site_name: str
    site_id: str
    id: str
    probe_id: str
    probe_measure: str
    soil_value: str
    unit: str
    json_featuretype: str
    date: Date
    time: str
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    daily_units_time: str
    daily_units_temperature_2m_mean: float
    daily_units_precipitation_sum: float
    daily_units_et0_fao_evapotranspiration: float
    daily_time: str
    daily_temperature_2m_mean: float
    daily_precipitation_sum: float
    daily_et0_fao_evapotranspiration: float
    soil_moisture_content: float
    soil_salinity: float
    soil_moisture_deficit: float
    GDD: float
    water_stress_index: float
    soil_moisture_depletion_rate: float
    average_soil_moisture_content: float
    cumulative_evapotranspiration: float
    cumulative_rainfall: float


def fetch_data(query: str) -> List[dict]:
    if USE_REMOTE_DB:
        con = duckdb.connect(
            f'md:[sensor_database]?[motherduck_token={MOTHERDUCK_TOKEN}]&saas_mode=true')
    else:
        con = duckdb.connect(duckdb_file)
    cursor = con.execute(query)
    columns = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    con.close()
    return [dict(zip(columns, row)) for row in results]

# Function to verify API key


def verify_api_key(api_key: str = Path(...)):
    expected_api_key = os.getenv("API_KEY")
    if api_key != expected_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


# 1. Get all data for specific site_id
@app.get("/api/sensor_data/{api_key}/{site_id}", response_model=List[dict])
async def get_all_data_site(api_key: str = Path(...), site_id: str = Path(...), authorized: bool = Depends(verify_api_key)):
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 2. Get all data for specific site_id and probe_id


@app.get("/api/sensor_data/{api_key}/{site_id}/{probe_id}", response_model=List[dict])
async def get_all_data_site_probe(api_key: str = Path(...), site_id: str = Path(...), probe_id: str = Path(...), authorized: bool = Depends(verify_api_key)):
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND probe_id = '{probe_id}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 3. Get all data for specific site_id and date


@app.get("/api/sensor_data/{api_key}/{site_id}/{date}", response_model=List[dict])
async def get_all_data_site_date(api_key: str = Path(...), site_id: str = Path(...), date: Date = Path(...), authorized: bool = Depends(verify_api_key)):
    formatted_date = date.isoformat()  # Convert Date object to ISO format
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND date = DATE '{formatted_date}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 3.1 Get all data for specific site_id between start_date and end_date


@app.get("/api/sensor_data/{api_key}/{site_id}/between_dates", response_model=List[dict])
async def get_all_data_site_between_dates(api_key: str = Path(...), site_id: str = Path(...), start_date: Date = Path(...), end_date: Date = Path(...), authorized: bool = Depends(verify_api_key)):
    formatted_start_date = start_date.isoformat()
    formatted_end_date = end_date.isoformat()
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND date BETWEEN DATE '{formatted_start_date}' AND DATE '{formatted_end_date}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 4. Get all data for specific site_id, probe_id, and date


@app.get("/api/sensor_data/{api_key}/{site_id}/{probe_id}/{date}", response_model=List[dict])
async def get_all_data_site_probe_date(api_key: str = Path(...), site_id: str = Path(...), probe_id: str = Path(...), date: Date = Path(...), authorized: bool = Depends(verify_api_key)):
    formatted_date = date.isoformat()
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND probe_id = '{probe_id}' AND date = DATE '{formatted_date}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 4.1 Get all data for specific site_id, probe_id between start_date and end_date


@app.get("/api/sensor_data/{api_key}/{site_id}/{probe_id}/between_dates", response_model=List[dict])
async def get_all_data_site_probe_between_dates(api_key: str = Path(...), site_id: str = Path(...), probe_id: str = Path(...), start_date: Date = Path(...), end_date: Date = Path(...), authorized: bool = Depends(verify_api_key)):
    formatted_start_date = start_date.isoformat()
    formatted_end_date = end_date.isoformat()
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND probe_id = '{probe_id}' AND date BETWEEN DATE '{formatted_start_date}' AND DATE '{formatted_end_date}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 5. Get all data for specific site_id, probe_id, date, and unit


@app.get("/api/sensor_data/{api_key}/{site_id}/{probe_id}/{date}/{unit}", response_model=List[dict])
async def get_all_data_site_probe_date_unit(api_key: str = Path(...), site_id: str = Path(...), probe_id: str = Path(...), date: Date = Path(...), unit: str = Path(...), authorized: bool = Depends(verify_api_key)):
    allowed_units = ["°C", "μS/cm", "%VWC"]
    if unit not in allowed_units:
        raise HTTPException(
            status_code=400, detail=f"Unit '{unit}' is not supported.")
    formatted_date = date.isoformat()
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND probe_id = '{probe_id}' AND date = DATE '{formatted_date}' AND unit = '{unit}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# 5.1 Get all data for specific site_id, probe_id, unit between start_date and end_date


@app.get("/api/sensor_data/{api_key}/{site_id}/{probe_id}/unit_between_dates", response_model=List[dict])
async def get_all_data_site_probe_unit_between_dates(api_key: str = Path(...), site_id: str = Path(...), probe_id: str = Path(...), unit: str = Path(...), start_date: Date = Path(...), end_date: Date = Path(...), authorized: bool = Depends(verify_api_key)):
    allowed_units = ["°C", "μS/cm", "%VWC"]
    if unit not in allowed_units:
        raise HTTPException(
            status_code=400, detail=f"Unit '{unit}' is not supported.")
    formatted_start_date = start_date.isoformat()
    formatted_end_date = end_date.isoformat()
    query = f"SELECT * FROM sensor_data WHERE site_id = '{site_id}' AND probe_id = '{probe_id}' AND unit = '{unit}' AND date BETWEEN DATE '{formatted_start_date}' AND DATE '{formatted_end_date}' ORDER BY date DESC LIMIT 30"
    return fetch_data(query)

# Endpoint to generate Swagger UI


@app.get("/docs")
async def get_swagger_ui():
    return {"msg": "Visit /docs to view Swagger UI."}

# Endpoint to generate ReDoc


@app.get("/redoc")
async def get_redoc():
    return {"msg": "Visit /redoc to view ReDoc."}
