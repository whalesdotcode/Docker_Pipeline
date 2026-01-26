import click
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "cbd_congestion_fee": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]



@click.command()
@click.option('--year', type=int, default=2021, help='Year of the data')
@click.option('--month', type=int, default=1, help='Month of the data')
@click.option('--pg-user', type=str, default='root', help='PostgreSQL username')
@click.option('--pg-pass', type=str, default='root', help='PostgreSQL password')
@click.option('--pg-host', type=str, default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg-db', type=str, default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', type=int, default=100000, help='Chunk size for data loading')
@click.option('--target-table', type=str, default='yellow_taxi_data', help='Target table name')
def run(year, month, pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    
    try:
        prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        suffix = f'green_tripdata_{year}-{month:02d}.parquet'
        print(f"Downloading {suffix}...")
        url = prefix + suffix
        df = pd.read_parquet(url)
        df = df.astype(dtype)
        df[parse_dates] = df[parse_dates].apply(pd.to_datetime)
        print(f"Downloaded successfully. Rows: {len(df)}")
    except FileNotFoundError:
        print(f"Error: File not found - {suffix}")
        return
    except Exception as e:
        print(f"Error downloading file: {e}")
        return

    try:
        engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
        print("Connected to database successfully")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
        engine.dispose()
        return

    try:
        for i in tqdm(range(0, len(df), chunksize)):
            chunk = df.iloc[i:i+chunksize]
            chunk.to_sql(target_table, con=engine, if_exists='append', index=False)
        print("Data upload completed successfully")
    except Exception as e:
        print(f"Error uploading data: {e}")
    finally:
        engine.dispose()
        print("Database connection closed")

if __name__ == '__main__':
    run()
