# main.py

from pyspark.sql import SparkSession
import os
from src.visualization import plot_top_countries
from src.visualization import (
    bar_plot_india_daily_cases,
    bar_plot_india_daily_recovered,
    bar_plot_india_daily_deaths
)



def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("COVID-19 Data Analysis") \
        .getOrCreate()

    # Step 2: Load the CSV file
    data_path = os.path.join("data", "covid_19_data.csv")
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Optional: Print schema to confirm
    print("📄 DataFrame Schema:")
    df.printSchema()

    # Step 3: Run visualizations
    plot_top_countries(df)
    bar_plot_india_daily_cases(df)
    bar_plot_india_daily_recovered(df)
    bar_plot_india_daily_deaths(df)



    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
