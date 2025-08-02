# visualization.py
import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import sum as spark_sum

# Bar chart: Top 10 countries by confirmed cases
def plot_top_countries(df):
    country_data = df.groupBy("Country").agg(spark_sum("Confirmed").alias("TotalConfirmed"))
    top_countries = country_data.orderBy("TotalConfirmed", ascending=False).limit(10)

    # Convert to Pandas for plotting
    top_countries_pd = top_countries.toPandas().set_index("Country")
    top_countries_pd.sort_values("TotalConfirmed", inplace=True)

    # Plot
    plt.figure(figsize=(10, 6))
    import seaborn as sns
    sns.barplot(
        x=top_countries_pd["TotalConfirmed"],
        y=top_countries_pd.index,
        hue=top_countries_pd.index,
        palette="Reds_d",
        legend=False
    )
    plt.title("Top 10 Countries by Confirmed COVID-19 Cases")
    plt.xlabel("Confirmed Cases")
    plt.ylabel("Country")
    plt.tight_layout()

    output_path = os.path.join("plots", "top_countries.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ Plot saved in '{output_path}'")

# Line chart: Daily confirmed cases in India
def plot_india_daily_cases(df):
    india_df = df.filter(df['Country'] == 'India')

    # Group by Date and sum Confirmed cases
    india_daily = india_df.groupBy("Date").agg(spark_sum("Confirmed").alias("TotalConfirmed"))

    # Convert to Pandas for plotting
    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    # Plot
    plt.figure(figsize=(12, 6))
    plt.plot(india_daily_pd['Date'], india_daily_pd['TotalConfirmed'], color='blue')
    plt.title("Daily Confirmed COVID-19 Cases in India")
    plt.xlabel("Date")
    plt.ylabel("Total Confirmed Cases")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_daily_confirmed.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily confirmed plot saved at '{output_path}'")

# Line chart: Daily death cases in India
def plot_india_daily_deaths(df):
    india_df = df.filter(df['Country'] == 'India')
    india_daily = india_df.groupBy("Date").agg(spark_sum("Deaths").alias("TotalDeaths"))

    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    plt.figure(figsize=(12, 6))
    plt.plot(india_daily_pd['Date'], india_daily_pd['TotalDeaths'], color='red')
    plt.title("Daily Deaths due to COVID-19 in India")
    plt.xlabel("Date")
    plt.ylabel("Total Deaths")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_daily_deaths.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily deaths plot saved at '{output_path}'")

# Line chart: Daily recovered cases in India
def plot_india_daily_recovered(df):
    india_df = df.filter(df['Country'] == 'India')
    india_daily = india_df.groupBy("Date").agg(spark_sum("Recovered").alias("TotalRecovered"))

    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    plt.figure(figsize=(12, 6))
    plt.plot(india_daily_pd['Date'], india_daily_pd['TotalRecovered'], color='green')
    plt.title("Daily Recovered COVID-19 Cases in India")
    plt.xlabel("Date")
    plt.ylabel("Total Recovered Cases")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_daily_recovered.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily recovered plot saved at '{output_path}'")

# Bar chart: Daily confirmed cases in India
def bar_plot_india_daily_cases(df):
    india_df = df.filter(df['Country'] == 'India')

    india_daily = india_df.groupBy("Date").agg(spark_sum("Confirmed").alias("TotalConfirmed"))

    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    plt.figure(figsize=(12, 6))
    plt.bar(india_daily_pd['Date'], india_daily_pd['TotalConfirmed'], color='green')
    plt.title("Daily Confirmed COVID-19 Cases in India (Bar Graph)")
    plt.xlabel("Date")
    plt.ylabel("Total Confirmed Cases")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_bar_confirmed.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily bar chart saved at '{output_path}'")


def bar_plot_india_daily_recovered(df):
    india_df = df.filter(df['Country'] == 'India')
    india_daily = india_df.groupBy("Date").agg(spark_sum("Recovered").alias("TotalRecovered"))

    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    plt.figure(figsize=(12, 6))
    plt.bar(india_daily_pd['Date'], india_daily_pd['TotalRecovered'], color='orange')
    plt.title("Daily Recovered COVID-19 Cases in India (Bar Graph)")
    plt.xlabel("Date")
    plt.ylabel("Total Recovered Cases")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_bar_recovered.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily recovered bar chart saved at '{output_path}'")


def bar_plot_india_daily_deaths(df):
    india_df = df.filter(df['Country'] == 'India')
    india_daily = india_df.groupBy("Date").agg(spark_sum("Deaths").alias("TotalDeaths"))

    india_daily_pd = india_daily.toPandas()
    india_daily_pd['Date'] = pd.to_datetime(india_daily_pd['Date'])
    india_daily_pd.sort_values('Date', inplace=True)

    plt.figure(figsize=(12, 6))
    plt.bar(india_daily_pd['Date'], india_daily_pd['TotalDeaths'], color='red')
    plt.title("Daily Deaths due to COVID-19 in India (Bar Graph)")
    plt.xlabel("Date")
    plt.ylabel("Total Deaths")
    plt.xticks(rotation=45)
    plt.tight_layout()

    output_path = os.path.join("plots", "india_bar_deaths.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ India daily deaths bar chart saved at '{output_path}'")
