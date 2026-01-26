# ingest_csv_to_docker_postgres.py
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

DB_USER     = "root"
DB_PASSWORD = "root"
DB_NAME     = "ny_taxi"
DB_HOST     = "postgres_db"  # Use "localhost" if running outside Docker
DB_PORT     = "5432"

connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(connection_string)

csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
df = pd.read_csv(csv_url)

df.to_sql(
    name       = 'zone',
    con        = engine,
    if_exists  = 'replace',
    index      = False,
    chunksize  = 10000
)

print("Upload finished!")

with engine.connect() as conn:
    result = conn.execute(text("SELECT count(*) FROM zone"))
    print("Rows in table:", result.scalar())