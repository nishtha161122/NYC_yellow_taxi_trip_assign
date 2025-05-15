# NYC_yellow_taxi_trip_assign
# Yellow Taxi Trips Data Analysis

This project involves analyzing Yellow Taxi trip data from New York City. The goal is to process the data, extract meaningful insights, and visualize trends. The project is organized into three main scripts:

## Data Source

The data is sourced from a PostgreSQL database (nyc_taxi_db), specifically the `yellow_taxi_trips` table.

## Project Steps

1.  **Data Loading:**
    * The `loaddata.py` script handles loading the data from the PostgreSQL database.
    * It checks if the `yellow_taxi_trips` table exists. If not, it creates the table and loads the data in the sql.

2.  **Data Cleaning and Feature Engineering:**
    * The `filtering.py` script performs data cleaning and feature engineering.
    * It includes the following operations:
        * Converts `pickup_datetime` and `dropoff_datetime` columns to datetime objects.
        * Removes rows with any missing values.
        * Removes outliers in the `trip_distance` and `fare_amount` columns using the IQR method.
        * Calculates new features:
            * `trip_duration`: Trip duration in minutes.
            * `speed`: Average speed of the trip in miles per hour.
            * `time_of_day`: Categorical variable indicating the time of day (morning, afternoon, evening, night) based on the pickup time.
            * `is_weekend`: Boolean variable indicating whether the trip started on a weekend.
        * It also adds the new columns (`trip_duration`, `speed`, `time_of_day`, `is_weekend`) to the database table.

3.  **Data Visualization:**
    * The `visualizing.py` script visualizes the daily total revenue.
    * It performs the following steps:
        * Loads `pickup_datetime` and `total_amount` from the database.
        * Calculates daily total revenue.
        * Calculates the 7-day moving average of the daily revenue.
        * Generates a line plot showing the daily revenue and the 7-day moving average.
        * Annotates major peaks and troughs in the daily revenue with their corresponding values.

## Libraries Used

* pandas
* sqlalchemy
* matplotlib
* seaborn
* os
* logging
* time

## Files

* `loaddata.py`:  Script for loading data from the PostgreSQL database.
* `filtering.py`: Script for cleaning the data and creating new features.
* `visualizing.py`: Script for visualizing the daily total revenue.
* `README.md`: This file, providing an overview of the project.

