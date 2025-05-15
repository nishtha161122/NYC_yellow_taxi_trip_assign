import pandas as pd
from sqlalchemy import create_engine, text  # Import the 'text' function
import logging
import time
import os

def clean_taxi_data(db_params, chunksize=100000, save_to_db=False, new_table_name="cleaned_taxi_trips", save_to_csv=False, csv_filename="cleaned_taxi_data.csv"):
 
    start_time = time.time()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        sql = """
            SELECT
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                fare_amount,
                payment_type,
                total_amount
            FROM yellow_taxi_trips
            """

        logging.info(f"Loading and cleaning data from PostgreSQL in chunks of {chunksize}...")
        all_chunks = []

        for chunk in pd.read_sql(sql, engine, chunksize=chunksize):
            logging.info(f"Processing chunk of shape: {chunk.shape}")

            chunk['pickup_datetime'] = pd.to_datetime(chunk['pickup_datetime'])
            chunk['dropoff_datetime'] = pd.to_datetime(chunk['dropoff_datetime'])

            rows_before = chunk.shape[0]
            chunk = chunk.dropna()
            rows_after = chunk.shape[0]
            logging.info(f"Removed {rows_before - rows_after} rows with missing values.")

            for col in ['trip_distance', 'fare_amount']:
                Q1 = chunk[col].quantile(0.25)
                Q3 = chunk[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                rows_before_outlier = chunk.shape[0]
                chunk = chunk[(chunk[col] >= lower_bound) & (chunk[col] <= upper_bound)]
                rows_after_outlier = chunk.shape[0]
                logging.info(f"Removed {rows_before_outlier - rows_after_outlier} outliers in {col}.")

            #  Calculate new features
            chunk['trip_duration'] = (chunk['dropoff_datetime'] - chunk['pickup_datetime']).dt.total_seconds() / 60  # in minutes
            chunk['speed'] = (chunk['trip_distance'] / chunk['trip_duration']) * 60  # miles per hour
            chunk['time_of_day'] = chunk['pickup_datetime'].dt.hour.apply(lambda x: 'morning' if 6 <= x < 12 else ('afternoon' if 12 <= x < 17 else ('evening' if 17 <= x < 22 else 'night')))
            chunk['is_weekend'] = chunk['pickup_datetime'].dt.dayofweek.isin([5, 6])  # 5: Saturday, 6: Sunday

            if not chunk.empty:
                all_chunks.append(chunk)

        df = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()

        logging.info(f"Final data shape after processing all chunks: {df.shape}")

        if not df.empty:
            logging.info("Verifying data types:")
            logging.info(df.dtypes)

            logging.info("Sample of cleaned data:")
            print(df.head())
        else:
            logging.info("No data to display after cleaning and filtering.")
            return df

        if save_to_csv:
            df.to_csv(csv_filename, index=False)
            logging.info(f"Cleaned data saved to '{csv_filename}'")
        elif save_to_db:
            try:
                #  Adding the colunms if they don't exist
                with engine.connect() as conn:
                    conn.execute(text("""  -- Wrap the SQL string with 'text()'
                        ALTER TABLE yellow_taxi_trips
                        ADD COLUMN IF NOT EXISTS trip_duration NUMERIC,
                        ADD COLUMN IF NOT EXISTS speed NUMERIC,
                        ADD COLUMN IF NOT EXISTS time_of_day VARCHAR(20),
                        ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN
                    """))
                df.to_sql(new_table_name, engine, if_exists='replace', index=False)
                logging.info(f"Cleaned data saved to table '{new_table_name}' in database.")
            except Exception as db_error:
                logging.error(f"Error saving to database: {db_error}")
                raise  

        return df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return pd.DataFrame()

    finally:
        end_time = time.time()
        logging.info(f"Total processing time: {end_time - start_time:.2f} seconds")



if __name__ == "__main__":
    db_params = {
        'dbname': 'nyc_taxi_db',
        'user': 'postgres',
        'password': os.environ.get('POSTGRES_PASSWORD', 'your_password'), #put your "chosen pass" at "your_pass"
        'host': 'localhost',
        'port': 5432
    }

    if db_params['password'] == 'your_password':
        logging.warning("Please set the POSTGRES_PASSWORD environment variable for security.")

    cleaned_df = clean_taxi_data(db_params, save_to_db=True, new_table_name="cleaned_yellow_taxi_trips") # set save_to_db to true
    if not cleaned_df.empty:
        print(cleaned_df.shape)
    else:
        print("No data returned from clean_taxi_data()")
