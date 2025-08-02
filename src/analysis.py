# src/analysis.py

def get_summary(df):
    """
    Returns basic summary statistics from the dataset.
    """
    summary = {
        "Total Records": len(df),
        "Total Countries": df['Country'].nunique(),
        "Total Confirmed Cases": df['Confirmed'].sum(),
        "Total Recovered Cases": df['Recovered'].sum(),
        "Total Deaths": df['Deaths'].sum(),
    }
    return summary
