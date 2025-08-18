# Weather Data Pipeline (Philippines)

A **Dockerized ETL pipeline** for fetching, storing, and visualizing real-time Philippine weather data.  
This project integrates **Python, Apache Airflow, PostgreSQL, and Power BI** to demonstrate a modern data engineering workflow.

---

## Project Overview
This pipeline automates the process of:
1. **Extract** — Pulling real-time weather data from the [OpenWeather API](https://openweathermap.org/api).
2. **Transform** — Cleaning and structuring the data (CSV stored locally).
3. **Load** — Inserting the data into a **PostgreSQL database** using Docker.
4. **Orchestrate** — Managing scheduled hourly runs with **Apache Airflow**.
5. **Visualize** — Connecting PostgreSQL directly to **Power BI** for dashboards.

---

## Tech Stack
- **Python** — API extraction and ETL scripts  
- **Docker & Docker Compose** — Containerized services  
- **PostgreSQL** — Data warehouse  
- **Apache Airflow** — Workflow orchestration  
- **Power BI** — Business intelligence dashboard  

---

## Documentation
The full documentation of the project can be accessed at https://medium.com/@francischan478/weather-data-pipeline-with-python-airflow-and-power-bi-cd179e2669fe



