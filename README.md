# 🚀 NASA Near-Earth Objects (NEO) Data Pipeline

A comprehensive data pipeline for processing NASA's Near-Earth Objects data using Apache Airflow, MinIO object storage, Apache Spark with Iceberg tables, Redis queuing, and interactive dashboards.

## 📋 Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Accessing Services](#accessing-services)
- [Running the Pipeline](#running-the-data-pipeline)
- [Using the Dashboard](#using-the-dashboard)
- [Architecture](#architecture)


## Overview

This project processes NASA's Near-Earth Objects data through a medallion architecture (Bronze → Silver → Gold) to create analytics-ready datasets. The pipeline extracts data from NASA's NEO API, transforms it using Apache Spark, stores it in Apache Iceberg tables, and provides interactive dashboards for data exploration.

### What are Near-Earth Objects?
Near-Earth Objects (NEOs) are comets and asteroids nudged by gravitational attraction into orbits that bring them close to Earth. This pipeline helps track and analyze these objects, including Potentially Hazardous Asteroids (PHAs) that pose potential threats to our planet.

## Prerequisites

- **Docker** (required)
- **Git** (required)
- **NASA API Key** (free - instructions below)

## Getting Started

### Step 1: Get NASA API Key

1. Visit the [NASA Open Data Portal](https://api.nasa.gov/)
2. Click "Get Started" and enter your information
3. Check your email for the API key
4. **Save this key** - you'll need it in Step 4

### Step 2: Clone the Repository

```bash
git clone <your-repository-url>
cd nasa_neo_pipeline
```

### Step 3: Build the Docker Image

```bash
docker build -t neo-pipeline .
```

### Step 4: Configure NASA API Key

Before running the container, you need to update the configuration file:

1. Open `src/config.py` in a text editor
2. Find the line: `NASA_NEO_API_KEY='<Add NASA OPEN API KEY HERE>'`
3. Replace `<Add NASA OPEN API KEY HERE>` with your actual NASA API key
4. Save the file

Example:
```python
NASA_NEO_API_KEY='YOUR_ACTUAL_API_KEY_HERE'
```

### Step 5: Run the Container

```bash
docker run -it -p 8080:8080 -p 9001:9001 -p 8888:8888 neo-pipeline
```

## Accessing Services

Once the container is running, you can access these web interfaces:

### Apache Airflow UI
- **URL**: http://localhost:8080
- **Purpose**: Orchestrate and monitor the ETL pipeline
- **Credentials**: Check the terminal output when Airflow starts - it will display the admin password
- **Default Username**: `admin`
- **Key DAG**: `NeoAPIPipeline` - This runs the main ETL process

### MinIO Object Storage Console  
- **URL**: http://localhost:9001
- **Purpose**: Browse raw data files and object storage
- **Username**: `minioadmin`
- **Password**: `minioadmin`

### Jupyter Notebook
- **URL**: http://localhost:8888
- **Purpose**: Run interactive dashboards and data analysis
- **Authentication**: Check terminal for the token URL, or use the token displayed in the startup logs

### Redis CLI Access
To access Redis from within the container:
```bash
# From inside the container
redis-cli

# Test the connection
ping
# Should return: PONG

# View queue contents
LLEN queue
# Shows number of items in default queue

# View all keys
KEYS *
```

## Running the Data Pipeline

### Step 1: Start the Pipeline in Airflow
1. Go to Airflow UI at http://localhost:8080
2. Find the `NeoAPIPipeline` DAG
3. Toggle it "On" if it's not already active
4. Click "Trigger DAG" to run it manually
5. **First run may take 10-15 minutes** as it processes historical data

### Pipeline Stages
The pipeline runs through these stages automatically:
1. **Generate Missing Dates**: Identifies data gaps to fill
2. **Bronze Processing**: Downloads raw JSON from NASA API
3. **Silver Processing**: Transforms JSON to structured Parquet tables  
4. **Gold Processing**: Creates analytics-ready aggregated data
5. **Cleanup**: Removes temporary files

## Using the Dashboard

**⚠️ Important**: Run the Airflow pipeline first (see "Running the Data Pipeline" above) to ensure data is available for the dashboard.

### Step 1: Open Jupyter Notebook
1. Go to http://localhost:8888
2. Use the token from the terminal output if prompted
3. Navigate to the `notebooks` folder
4. Open `Dashboard_Panel.ipynb`

### Step 2: Run the Dashboard
1. Execute all cells in order (Cell → Run All)
2. Wait for the Spark session to initialize (may take 1-2 minutes)
3. The interactive dashboard will appear at the bottom

### Step 3: Interact with the Data
The dashboard provides two main visualizations:

#### Miss Distance Chart (Bar Chart)
- **What it shows**: How close each asteroid came to Earth (measured in lunar distances)
- **Y-axis**: `miss_distance_ld` (lunar distances - 1 LD ≈ 384,400 km)
- **X-axis**: Asteroid names (e.g., "2024 BF", "2025 GT1")
- **How to use**: Use the date slider to see different observation dates

#### Velocity Distribution (Histogram)
- **What it shows**: Distribution of asteroid velocities in km/s
- **X-axis**: `velocity_kps` (kilometers per second)
- **Y-axis**: Count of asteroids in each velocity range
- **How to use**: Histogram updates based on selected date

#### Date Slider
- **Function**: Filter data by observation date
- **Range**: Covers data from May 1, 2025, onwards (configurable in `src/date_ranges.py`)
- **Real-time**: Charts update immediately when you change the date
- **Data Availability**: Only dates with processed data will show results

## Architecture

### Data Flow (Medallion Architecture)
1. **Bronze Layer**: Raw JSON data from NASA API → MinIO storage
2. **Silver Layer**: Structured Parquet data → Iceberg tables  
3. **Gold Layer**: Analytics-ready aggregated data → Dashboard consumption

### Technology Stack
- **Orchestration**: Apache Airflow
- **Storage**: MinIO (S3-compatible object storage)
- **Processing**: Apache Spark with PySpark
- **Data Format**: Apache Iceberg tables
- **Queuing**: Redis
- **Visualization**: Panel + Jupyter
- **Containerization**: Docker
