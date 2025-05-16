import pandas as pd # import the pandad library( used to manipulate data)
import psycopg2 #import psycopg2, a postgresql adapter for python
from sqlalchemy import create_engine #import create engine from sqlalchemy, used to connect to database.
import time #import time module, used to calculate the execution time for the operation.
import os #import os module, to interact witht the os.

def load_parquet_to_postgres(parquet_file, db_params, table_name):
    start_time = time.time() #record the starting time of the operation

    # 1. Check if the Parquet file exists
    if not os.path.exists(parquet_file): #check if the parquet file exists.
        raise FileNotFoundError(f"Error: Parquet file not found at {parquet_file}") #raise an error if file is not found.

    # 2. Read the Parquet file into a Pandas DataFrame
    print("Reading Parquet file...")
    df = pd.read_parquet(parquet_file) #use pandas to read the parquet file into dataframe.
    print(f"Loaded {len(df)} rows from Parquet file") #print the number of rows read from the file.

    # 3. Standardize and Rename columns
    print("Standardizing and renaming columns...")
    df.columns = [col.lower().replace(' ', '_') for col in df.columns] #convert column names to lowercase and replace spaces with underscores
    column_mapping = { #define a dictionary to map original names to new names.
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
    df = df.rename(columns=column_mapping)#rename the columns using mapping distionary

    # 4. Connect to PostgreSQL and Create Table if needed
    print("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(**db_params) # Establish a connection to the PostgreSQL database using psycopg2. The connection parameters are unpacked from the 'db_params' dictionary.
        cursor = conn.cursor()# Create a cursor object, which allows to execute SQL queries.
        conn.autocommit = True # Set autocommit to true

        # Check if the table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,)) #check if the table exists.
        table_exists = cursor.fetchone()[0]#fetch result of query and extract first elemtn.

        if not table_exists: #if table doesnt exist.
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
            cursor.execute(create_table_sql)#execute the create table 
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

        cursor.close()  # Close the cursor here, we'll use a separate connection for SQLAlchemy
        conn.close()

        # 5. Load data using SQLAlchemy in chunks
        print("Loading data into PostgreSQL...")
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}") #creating sqlalchemy engine for connection.
        chunk_size = 200000  # Adjust chunk size as needed
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            end = min(i + chunk_size, total_rows)
            print(f"Loading rows {i} to {end} of {total_rows}...")
            df_chunk = df.iloc[i:end]
            df_chunk.to_sql(table_name, engine, if_exists='append', index=False)

        print(f"Data loaded successfully into table {table_name}.")

    except Exception as e: #catch any exceptions that occur
        print(f"Error: {e}") #print error text.
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()#connection is closed

    end_time = time.time()#record the ending time
    duration = end_time - start_time#total execution time
    print(f"Loading took {duration:.2f} seconds")#printing execution time

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
