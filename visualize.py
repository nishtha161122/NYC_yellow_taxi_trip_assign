import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os

def visualize_taxi_revenue(db_params, table_name="yellow_taxi_trips"):

    try:
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

        # load data
        sql = f"SELECT pickup_datetime, total_amount FROM {table_name}"
        df = pd.read_sql(sql, engine)

        # prep data
        df['pickup_date'] = df['pickup_datetime'].dt.date  # Extract date part
        daily_revenue = df.groupby('pickup_date')['total_amount'].sum().reset_index()
        daily_revenue['pickup_date'] = pd.to_datetime(daily_revenue['pickup_date'])  # Ensure datetime

        # calc avg
        daily_revenue['7_day_avg'] = daily_revenue['total_amount'].rolling(window=7, min_periods=1).mean()

        # to fnd peaks and troughs
        peaks = daily_revenue[(daily_revenue['total_amount'].shift(1) < daily_revenue['total_amount']) & (daily_revenue['total_amount'].shift(-1) < daily_revenue['total_amount'])]
        troughs = daily_revenue[(daily_revenue['total_amount'].shift(1) > daily_revenue['total_amount']) & (daily_revenue['total_amount'].shift(-1) > daily_revenue['total_amount'])]

        # plot
        plt.figure(figsize=(15, 7))
        sns.lineplot(x='pickup_date', y='total_amount', data=daily_revenue, label='Daily Revenue', color='blue')
        sns.lineplot(x='pickup_date', y='7_day_avg', data=daily_revenue, label='7-Day Moving Average', color='red')

        # annotation
        for index, row in peaks.iterrows():
            plt.annotate(f'Peak: ${row["total_amount"]:.2f}',
                         xy=(row['pickup_date'], row['total_amount']),
                         xytext=(row['pickup_date'] + pd.Timedelta(days=7), row['total_amount'] + 100),  
                         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2'))
        for index, row in troughs.iterrows():
            plt.annotate(f'Trough: ${row["total_amount"]:.2f}',
                         xy=(row['pickup_date'], row['total_amount']),
                         xytext=(row['pickup_date'] + pd.Timedelta(days=7), row['total_amount'] - 100),  
                         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.2'))

        # labels/titles
        plt.xlabel('Date')
        plt.ylabel('Total Revenue')
        plt.title('Daily Total Revenue with 7-Day Moving Average')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()  
        plt.show()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Database connections
    db_params = {
        'dbname': 'nyc_taxi_db',
        'user': 'postgres',
        'password': os.environ.get('POSTGRES_PASSWORD', 'your_password'), #add 'your password' at 'your_password"
        'host': 'localhost',
        'port': 5432
    }

    if db_params['password'] == 'your_password':
        print("Please set the POSTGRES_PASSWORD environment variable for security.")

    visualize_taxi_revenue(db_params)
