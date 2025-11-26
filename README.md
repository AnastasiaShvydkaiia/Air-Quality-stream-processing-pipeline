# Air Quality stream processing pipeline

A fully containerized **data engineering** project for real-time monitoring and analysis of urban air quality. This pipeline simulates IoT sensors, streams environmental data through **Kafka** and **Spark Structured Streaming**, stores and aggregates data in **MongoDB**, manages retention policies with **Airflow**, and visualizes metrics using **Prometheus** and **Grafana**.

## Project Structure

```bash
├── airflow/
│   ├── dags/                           
│   │   └── data_retention.py           # Airflow DAG to handle data retention
│   ├── requirements.txt                # Python dependencies for Airflow container
│   └── Dockerfile                      # Dockerfile to build Airflow image with required dependencies
│
├── spark-apps/
│   ├── stream_processing.py            # PySpark script for real-time stream processing from Kafka
│   ├── start-spark.sh                  # Script to launch Spark jobs in container
│   ├── spark-jmx.yaml                  # JMX metrics configuration for Spark monitoring
│   └── Dockerfile                      # Dockerfile to build Spark image with connectors 
│
├── sensors/
│   ├── Dockerfile                      # Dockerfile to build sensor simulator container
│   ├── requirements.txt                # Python dependencies for sensor simulator
│   ├── sensor_data.csv                 # Sample or test sensor data used by the simulator
│   └── stream_producing.py             # Script to simulate sensors and publish data to Kafka
│
├── prometheus/
│   └── prometheus.yml                  # Prometheus configuration for scraping metrics
│
├── grafana/
│   ├── dashboards/                     # Folder containing Grafana dashboards in JSON format
│   │   ├── air-quality_dashboard.json  # Dashboard for visualizing air quality metrics
│   │   ├── kafka-dashboard.json        # Dashboard for monitoring Kafka metrics
│   │   ├── mongo-dashboard.json        # Dashboard for monitoring MongoDB metrics
│   │   └── spark-dashboard.json        # Dashboard for monitoring Spark metrics
│   └── provisioning/                   # Grafana provisioning folder
│       ├── dashboards/                 
│       │   └── dashboard.yml           # Dashboard provisioning configuration
│       └── datasources/               
│           └── datasource.yml          # Data source definition for Grafana 
│
├── img/                                # Folder containing images for documentation
│
├── docker-compose.yml                  # Docker Compose file to orchestrate the full pipeline
├── EDA.ipynb                           # Jupyter notebook for exploratory data analysis
└── README.md                           # Project documentation
```

## Project Overview

Sensors installed in a city measure various environmental parameters:

- **PM10 (µg/m³)**
- **PM2.5 (µg/m³)**
- **Pressure**
- **Temperature**
- **Humidity**
- **CO (ppm)**
- **NO₂ (ppm)**
- **O₃ (ppb)**

These measurements (sourced from [Kaggle Dataset](https://www.kaggle.com/datasets/leonardocosmote/air-quality-and-environmental-dataset-outdoor-1yr)) are streamed, processed, aggregated, and analyzed automatically.

### Air Quality Index (AQI) Calculation

The Air Quality Index (AQI) is a standardized indicator that converts pollutant concentrations into a scale describing the health impact of air quality.

### Reference Standards

This project follows the **U.S. EPA (Environmental Protection Agency)** methodology, as described in:
- [Technical Assistance Document for the Reporting of Daily Air Quality – the Air Quality Index (AQI) (EPA-454/B-24-002 2024)](https://document.airnow.gov/technical-assistance-document-for-the-reporting-of-daily-air-quailty.pdf)

### Formula

For each pollutant, the sub-index $I_p$
is calculated as:
$$I_p = \frac{I_{HI} - I_{LO}}{BP_{HI} - BP_{LO}} \times (C_p - BP_{LO}) + I_{LO}$$ \
Where:
- $C_p$ = observed concentration of pollutant *p*
- $BP_{HI}$, $BP_{LO}$ = upper and lower breakpoint concentrations for *p*
- $I_{HI}$, $I_{LO}$= corresponding AQI values for those breakpoints

The **overall AQI** is the **maximum of all sub-indices**:
$$AQI = \max(I_{PM2.5}, I_{PM10}, I_{NO2}, I_{O3}, I_{CO})$$

### U.S. EPA Breakpoints

| Category | AQI Range | PM2.5 (µg/m³) | PM10 (µg/m³) | NO₂ (ppm) | O₃ (ppb) |
|-----------|-----------|---------------|---------------|-------------|---------------|
| 🟢 Good | 0–50 | 0.0–12.0 | 0–54 | 0.000–0.053 | 0–54 |
| 🟡 Moderate | 51–100 | 12.1–35.4 | 55–154 | 0.054–0.100 | 55–70 | 
| 🟠 Unhealthy for Sensitive Groups | 101–150 | 35.5–55.4 | 155–254 | 0.101–0.360 | 71–85 | 
| 🔴 Unhealthy | 151–200 | 55.5–150.4 | 255–354 | 0.361–0.649 | 86–105 | 
| 🟣 Very Unhealthy | 201–300 | 150.5–250.4 | 355–424 | 0.650–1.249 | 106–200 |
| ⚫ Hazardous | 301–500 | 250.5–500.4 | 425–604 | 1.250+ | 201+ | 

---

## System Architecture

![Arch](img/architecture.png)

| Component  | Purpose  |
|------------|----------|
| **Apache Kafka (KRaft)** | Simulates real-time sensor data stream |
| **Apache Spark Structured Streaming** | Processes streaming data, aggregates by hour, computes AQI |
| **MongoDB** | Stores raw and aggregated sensor data |
| **Apache Airflow** | Implements data retention policy|
| **Prometheus** | Collects metrics from Spark, Kafka, MongoDB |
| **Grafana** | Visualizes air quality, AQI categories, and system metrics |
| **Docker & Docker Compose** | Containerized deployment of the entire data pipeline |

Air quality Grafana dashboard 

![Dashboard](img/example_dashboard.png)

### **Database:** `aq_db`

- `raw_collection` -> raw data from Kafka (every 15 seconds)  
- `agg_collection` -> hourly aggregates and calculated AQI  

## Usage

0. **Prerequisites**

- Docker & Docker Compose
- Python 3.11+

1. **Clone the repo:**

```git clone https://github.com/AnastasiaShvydkaiia/Air-Quality-stream-processing-pipeline.git```

2. **Start the system:**

```docker-compose up --build -d```

3. **Access:**

Service    	 |URL	                 |Credentials        |
|------------|---------------------|-------------------|
Mongo Express|http://localhost:8088|admin / admin      |
Airflow	     |http://localhost:8080|admin / admin      |
Grafana    	 |http://localhost:3000|admin / admin      |
Prometheus   |http://localhost:9090|-                  |
Spark        |http://localhost:4040|-                  |

4. **Stop**


```docker-compose down -v``` 



