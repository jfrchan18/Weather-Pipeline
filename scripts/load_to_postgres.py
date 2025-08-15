import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --- Config ---
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "weatherdb")
DB_USER = os.getenv("DB_USER", "weatheruser")
DB_PASS = os.getenv("DB_PASS", "weatherpass")
CSV_PATH = os.getenv("CSV_PATH", "data/clean_weather_latest.csv")

# --- 1) Read & normalize ---
df = pd.read_csv(CSV_PATH)

# unify timestamp column name
if "datetime" in df.columns:
    df = df.rename(columns={"datetime": "observed_at"})
elif "timestamp" in df.columns:
    df = df.rename(columns={"timestamp": "observed_at"})

# ensure required columns exist
for col in ["city", "observed_at", "temperature", "feels_like", "humidity", "wind_speed", "weather"]:
    if col not in df.columns:
        df[col] = np.nan

# parse timestamp as UTC-aware
df["observed_at"] = pd.to_datetime(df["observed_at"], utc=True, errors="coerce")

# drop rows with no city or timestamp
df = df.dropna(subset=["city", "observed_at"])

# de-dup within this batch to reduce ON CONFLICT churn
df = df.drop_duplicates(subset=["city", "observed_at"])

# replace NaN with None for psycopg2 NULLs
df = df.replace({np.nan: None})

records = list(
    df[["city", "observed_at", "temperature", "feels_like", "humidity", "wind_speed", "weather"]]
    .itertuples(index=False, name=None)
)

if not records:
    print("No records to load. Exiting cleanly.")
    raise SystemExit(0)

# --- 2) Connect & ensure schema ---
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
)
try:
    conn.autocommit = False
    with conn.cursor() as cur:
        # be a good citizen: prevent runaway queries
        cur.execute("SET application_name = 'weather_csv_loader';")
        cur.execute("SET statement_timeout = '60s';")

        # table definition
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city        TEXT NOT NULL,
            observed_at TIMESTAMPTZ,
            temperature DOUBLE PRECISION,
            feels_like  DOUBLE PRECISION,
            humidity    INTEGER,
            wind_speed  DOUBLE PRECISION,
            weather     TEXT
        );
        """)
        # make sure observed_at exists (for older tables)
        cur.execute("ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE weather_data ALTER COLUMN city SET NOT NULL;")

        # unique key + a CHECK that observed_at is UTC-aware (offset present)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'ux_weather_city_observed_at'
            ) THEN
                ALTER TABLE weather_data
                ADD CONSTRAINT ux_weather_city_observed_at UNIQUE (city, observed_at);
            END IF;
        END $$;
        """)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'ck_weather_observed_at_tz'
            ) THEN
                ALTER TABLE weather_data
                ADD CONSTRAINT ck_weather_observed_at_tz
                CHECK (observed_at IS NULL OR (extract(timezone FROM observed_at) IS NOT NULL));
            END IF;
        END $$;
        """)

        # helpful index for BI time filtering
        cur.execute("CREATE INDEX IF NOT EXISTS weather_data_observed_idx ON weather_data (observed_at);")

        # --- 3) Bulk UPSERT ---
        execute_values(cur, """
            INSERT INTO weather_data
                (city, observed_at, temperature, feels_like, humidity, wind_speed, weather)
            VALUES %s
            ON CONFLICT (city, observed_at) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                feels_like  = EXCLUDED.feels_like,
                humidity    = EXCLUDED.humidity,
                wind_speed  = EXCLUDED.wind_speed,
                weather     = EXCLUDED.weather;
        """, records, page_size=1000)

    conn.commit()
    print(f"✅ Upserted {len(records)} rows into weather_data")
finally:
    conn.close()
