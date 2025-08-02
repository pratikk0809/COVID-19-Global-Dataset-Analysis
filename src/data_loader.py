# src/data_loader.py

import pandas as pd

def load_data(file_path='data/covid_19_data.csv'):
    """
    Loads the COVID-19 dataset.
    """
    df = pd.read_csv(file_path)
    return df
