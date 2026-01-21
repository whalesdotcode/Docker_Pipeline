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
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]





def run():
    year = 2021
    month = 1

    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'
    chunksize = 100000
    target_table = 'yellow_taxi_data'
    
    try:
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
        suffix = f'yellow_tripdata_{year}-{month:02d}.csv.gz'
        print(f"Downloading {suffix}...")
        df = pd.read_csv(
            prefix + suffix,
            dtype=dtype,
            parse_dates=parse_dates
        )
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
        print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
        df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
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
