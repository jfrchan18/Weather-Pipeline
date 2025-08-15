# airflow/dags/weather_extractor_hourly.py
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="weather_extractor_hourly",
    description="Fetch multi-city PH weather data (OpenWeather) and upsert into Postgres",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # hourly
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather","extract"],
) as dag:

    run_extractor = BashOperator(
        task_id="run_extractor",
        bash_command="python /opt/airflow/scripts/extract_weather.py",
    )

    run_extractor
