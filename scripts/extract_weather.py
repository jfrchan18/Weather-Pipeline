import os, time
from datetime import datetime, timezone
import requests, psycopg2
from psycopg2.extras import execute_batch

API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "weatherdb")
DB_USER = os.getenv("DB_USER", "weatheruser")
DB_PASS = os.getenv("DB_PASS", "weatherpass")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
RATE_DELAY_SEC = float(os.getenv("RATE_DELAY_SEC", "1.2"))
MAX_RETRIES = 3

# (display_name, query_string_for_API)
CITIES = [
    ("Batanes", "Basco,PH"),
    ("Aparri", "Aparri,PH"),
    ("Vigan", "Vigan City,PH"),
    ("Laoag", "Laoag City,PH"),
    ("La Union", "San Fernando City,PH"),
    ("Baguio", "Baguio,PH"),
    ("Tuguegarao", "Tuguegarao City,PH"),
    ("Bataan", "Balanga,PH"),
    ("Dagupan City", "Dagupan,PH"),
    ("Subic", "Subic,PH"),
    ("Tarlac", "Tarlac City,PH"),
    ("Bocaue", "Bocaue,PH"),              
    ("Manila", "Manila,PH"),
    ("Taytay", "Taytay,PH"),
    ("Los Banos", "Los Baños,PH"),
    ("Sto Tomas", "Santo Tomas,PH"),
    ("Tagaytay", "Tagaytay,PH"),
    ("Lucena", "Lucena City,PH"),
    ("Naga", "Naga City,PH"),
    ("Puerto Galera", "Puerto Galera,PH"),
    ("Tacloban", "Tacloban City,PH"),
    ("Cebu City", "Cebu City,PH"),
    ("Bohol (Tagbilaran)", "Tagbilaran City,PH"),
    ("Iloilo", "Iloilo City,PH"),
    ("Bacolod", "Bacolod City,PH"),
    ("Puerto Princesa", "Puerto Princesa,PH"),
    ("Butuan", "Butuan City,PH"),
    ("Cagayan De Oro", "Cagayan de Oro,PH"),
    ("Davao City", "Davao City,PH"),
    ("Zamboanga City", "Zamboanga City,PH"),
]

def ow_get(session, params):
    """GET with basic retry/backoff for 429/5xx."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(BASE_URL, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} {r.text}")
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            sleep_s = RATE_DELAY_SEC * (2 ** (attempt - 1))
            print(f"⚠️  Retry {attempt}/{MAX_RETRIES} after error: {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

def ensure_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
      city_id        BIGINT,
      city_name      TEXT,
      country        TEXT,
      lat            DOUBLE PRECISION,
      lon            DOUBLE PRECISION,
      temperature_c  DOUBLE PRECISION,
      humidity_pct   INTEGER,
      pressure_hpa   INTEGER,
      wind_mps       DOUBLE PRECISION,
      clouds_pct     INTEGER,
      rain_1h_mm     DOUBLE PRECISION,
      rain_3h_mm     DOUBLE PRECISION,
      weather        TEXT,
      observed_at    TIMESTAMPTZ,           -- from API 'dt' (UTC)
      fetched_at     TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (city_id, observed_at)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS weather_data_observed_idx ON weather_data (observed_at);")

def main():
    if not API_KEY:
        raise RuntimeError("OPENWEATHER_API_KEY not set")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    ensure_table(cur)
    conn.commit()

    rows = []
    session = requests.Session()
    for display_name, query_str in CITIES:
        try:
            params = {"q": query_str, "appid": API_KEY, "units": "metric"}
            data = ow_get(session, params)

            city_id = data.get("id")
            city_name = data.get("name") or display_name
            sys = data.get("sys", {})
            country = sys.get("country")
            coord = data.get("coord", {})
            lat, lon = coord.get("lat"), coord.get("lon")

            main = data.get("main", {})
            wind = data.get("wind", {})
            clouds = data.get("clouds", {})
            rain = data.get("rain", {})

            temperature = main.get("temp")
            humidity = main.get("humidity")
            pressure = main.get("pressure")
            wind_mps = wind.get("speed")
            clouds_pct = clouds.get("all")
            rain_1h = rain.get("1h")
            rain_3h = rain.get("3h")
            weather_desc = (data.get("weather") or [{}])[0].get("description")

            # Observed time from API (UTC); fallback to now(UTC)
            dt_unix = data.get("dt")
            observed_at = datetime.fromtimestamp(dt_unix, tz=timezone.utc) if dt_unix \
                          else datetime.now(timezone.utc)

            rows.append((
                city_id, city_name, country, lat, lon,
                temperature, humidity, pressure, wind_mps, clouds_pct,
                rain_1h, rain_3h, weather_desc, observed_at
            ))

            print(f"✅ {city_name} ({country}) @ {observed_at.isoformat()}  "
                  f"temp={temperature}°C hum={humidity}% wind={wind_mps}m/s")

        except Exception as e:
            print(f"⚠️  Skipping {display_name} ({query_str}): {e}")

        time.sleep(RATE_DELAY_SEC)

    if rows:
        execute_batch(cur, """
            INSERT INTO weather_data(
              city_id, city_name, country, lat, lon,
              temperature_c, humidity_pct, pressure_hpa, wind_mps, clouds_pct,
              rain_1h_mm, rain_3h_mm, weather, observed_at
            )
            VALUES (
              %s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,
              %s,%s,%s,%s
            )
            ON CONFLICT (city_id, observed_at) DO UPDATE SET
              city_name=EXCLUDED.city_name,
              country=EXCLUDED.country,
              lat=EXCLUDED.lat,
              lon=EXCLUDED.lon,
              temperature_c=EXCLUDED.temperature_c,
              humidity_pct=EXCLUDED.humidity_pct,
              pressure_hpa=EXCLUDED.pressure_hpa,
              wind_mps=EXCLUDED.wind_mps,
              clouds_pct=EXCLUDED.clouds_pct,
              rain_1h_mm=EXCLUDED.rain_1h_mm,
              rain_3h_mm=EXCLUDED.rain_3h_mm,
              weather=EXCLUDED.weather;
        """, rows, page_size=50)

    conn.commit()
    cur.close(); conn.close()
    print(f"Done. Upserted rows: {len(rows)}/{len(CITIES)}")

if __name__ == "__main__":
    main()
