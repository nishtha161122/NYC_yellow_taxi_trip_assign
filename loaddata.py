import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time
import os

def load_parquet_to_postgres(parquet_file, db_params, table_name):
    """
    Loads Parquet data into a PostgreSQL table, creating the table if it doesn't exist.

    Args:
        parquet_file (str): Path to the Parquet file.
        db_params (dict): Dictionary containing PostgreSQL connection parameters
                         (dbname, user, password, host, port).
        table_name (str): Name of the table to load data into.
    """
    start_time = time.time()

    # 1. Check if the Parquet file exists
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Error: Parquet file not found at {parquet_file}")

    # 2. Read the Parquet file into a Pandas DataFrame
    print("Reading Parquet file...")
    df = pd.read_parquet(parquet_file)
    print(f"Loaded {len(df)} rows from Parquet file")

    # 3. Standardize and Rename columns
    print("Standardizing and renaming columns...")
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    column_mapping = {
        'vendorid': 'vendor_id',
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'ratecodeid': 'rate_code_id',
        'store_and_fwd_flag': 'store_and_fwd_flag',
        'pulocationid': 'pickup_location_id',
        'dolocationid': 'dropoff_location_id',
        'payment_type': 'payment_type',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'congestion_surcharge': 'congestion_surcharge',
        'airport_fee': 'airport_fee'
    }
    df = df.rename(columns=column_mapping)

    # 4. Connect to PostgreSQL and Create Table if needed
    print("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        conn.autocommit = True

        # Check if the table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print(f"Creating table {table_name}...")
            create_table_sql = """
                CREATE TABLE yellow_taxi_trips (
                    vendor_id SMALLINT,
                    pickup_datetime TIMESTAMP,
                    dropoff_datetime TIMESTAMP,
                    passenger_count FLOAT,
                    trip_distance NUMERIC,
                    rate_code_id SMALLINT,
                    store_and_fwd_flag VARCHAR(1),
                    pickup_location_id INTEGER,
                    dropoff_location_id INTEGER,
                    payment_type SMALLINT,
                    fare_amount NUMERIC,
                    extra NUMERIC,
                    mta_tax NUMERIC,
                    tip_amount NUMERIC,
                    tolls_amount NUMERIC,
                    improvement_surcharge NUMERIC,
                    total_amount NUMERIC,
                    congestion_surcharge NUMERIC,
                    airport_fee NUMERIC
                );
                """
            cursor.execute(create_table_sql)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

        cursor.close()  # Close the cursor here, we'll use a separate connection for SQLAlchemy
        conn.close()

        # 5. Load data using SQLAlchemy in chunks
        print("Loading data into PostgreSQL...")
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        chunk_size = 200000  # Adjust chunk size as needed
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            end = min(i + chunk_size, total_rows)
            print(f"Loading rows {i} to {end} of {total_rows}...")
            df_chunk = df.iloc[i:end]
            df_chunk.to_sql(table_name, engine, if_exists='append', index=False)

        print(f"Data loaded successfully into table {table_name}.")

    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

    end_time = time.time()
    duration = end_time - start_time
    print(f"Loading took {duration:.2f} seconds")

if __name__ == "__main__":
    # File path
    parquet_file = r"C:\Users\Nishtha\Desktop\Assignment_1\yellow_tripdata_2023-01.parquet"

    # Database connection parameters (IMPORTANT:  Move password out of script for security)
    db_params = {
        'dbname': 'nyc_taxi_db',
        'user': 'postgres',
        'password': '15231915',  # Replace with your actual password
        'host': 'localhost',
        'port': 5432
    }
    table_name = 'yellow_taxi_trips'  # Name of your table

    load_parquet_to_postgres(parquet_file, db_params, table_name)