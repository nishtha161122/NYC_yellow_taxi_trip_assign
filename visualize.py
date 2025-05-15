import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os

def visualize_taxi_revenue(db_params, table_name="yellow_taxi_trips"):
    """
    Visualizes daily total revenue with a 7-day moving average, peaks, and troughs.

    Args:
        db_params (dict): Database connection parameters.
        table_name (str): Name of the table containing the taxi data.
    """
    try:
        # 1. Create SQLAlchemy engine
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

        # 2. Load data from the database
        sql = f"SELECT pickup_datetime, total_amount FROM {table_name}"
        df = pd.read_sql(sql, engine)

        # 3. Prepare the data
        df['pickup_date'] = df['pickup_datetime'].dt.date  # Extract date part
        daily_revenue = df.groupby('pickup_date')['total_amount'].sum().reset_index()
        daily_revenue['pickup_date'] = pd.to_datetime(daily_revenue['pickup_date'])  # Ensure datetime

        # 4. Calculate 7-day moving average
        daily_revenue['7_day_avg'] = daily_revenue['total_amount'].rolling(window=7, min_periods=1).mean()

        # 5. Find peaks and troughs
        peaks = daily_revenue[(daily_revenue['total_amount'].shift(1) < daily_revenue['total_amount']) & (daily_revenue['total_amount'].shift(-1) < daily_revenue['total_amount'])]
        troughs = daily_revenue[(daily_revenue['total_amount'].shift(1) > daily_revenue['total_amount']) & (daily_revenue['total_amount'].shift(-1) > daily_revenue['total_amount'])]

        # 6. Create the plot
        plt.figure(figsize=(15, 7))
        sns.lineplot(x='pickup_date', y='total_amount', data=daily_revenue, label='Daily Revenue', color='blue')
        sns.lineplot(x='pickup_date', y='7_day_avg', data=daily_revenue, label='7-Day Moving Average', color='red')

        # 7. Annotate peaks and troughs
        for index, row in peaks.iterrows():
            plt.annotate(f'Peak: ${row["total_amount"]:.2f}',
                         xy=(row['pickup_date'], row['total_amount']),
                         xytext=(row['pickup_date'] + pd.Timedelta(days=5), row['total_amount'] + 100),  # Adjust offset as needed
                         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2'))
        for index, row in troughs.iterrows():
            plt.annotate(f'Trough: ${row["total_amount"]:.2f}',
                         xy=(row['pickup_date'], row['total_amount']),
                         xytext=(row['pickup_date'] + pd.Timedelta(days=5), row['total_amount'] - 100),  # Adjust offset as needed
                         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.2'))

        # 8. Set labels and title
        plt.xlabel('Date')
        plt.ylabel('Total Revenue')
        plt.title('Daily Total Revenue with 7-Day Moving Average')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()  # Adjust layout to prevent labels from overlapping
        plt.show()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Database connection parameters
    db_params = {
        'dbname': 'nyc_taxi_db',
        'user': 'postgres',
        'password': os.environ.get('POSTGRES_PASSWORD', '15231915'),
        'host': 'localhost',
        'port': 5432
    }

    if db_params['password'] == 'your_password':
        print("Please set the POSTGRES_PASSWORD environment variable for security.")

    visualize_taxi_revenue(db_params)
