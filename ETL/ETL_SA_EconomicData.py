import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fredapi import Fred
from dotenv import load_dotenv
import os

# Setup key securely
load_dotenv()
fred = Fred(api_key=os.getenv("fred_key"))


# E
def extract_data():
    # Monthly series
    construction_growth = fred.get_series(
        "ZAFPRCNTO01MLSAM", observation_start="2000-01-01", observation_end="2019-12-31"
    )
    manufacturing_growth = fred.get_series(
        "ZAFPRMNTO01GYSAM", observation_start="2000-01-01", observation_end="2019-12-31"
    )
    mining_growth = fred.get_series(
        "ZAFPIEAMI02GPM", observation_start="2000-01-01", observation_end="2019-12-31"
    )
    interest_rate = fred.get_series(
        "INTDSRZAM193N", observation_start="2000-01-01", observation_end="2019-12-31"
    )

    # Resample monthly series to yearly (using 'YE' instead of 'Y')
    construction_growth = construction_growth.resample("YE").mean()
    manufacturing_growth = manufacturing_growth.resample("YE").mean()
    mining_growth = mining_growth.resample("YE").mean()
    interest_rate = interest_rate.resample("YE").mean()

    # Yearly series
    inflation = fred.get_series(
        "FPCPITOTLZGZAF", observation_start="2000-01-01", observation_end="2019-12-31"
    )
    real_gdp = fred.get_series(
        "ZAFNGDPRPCPCPPPT", observation_start="2000-01-01", observation_end="2019-12-31"
    )
    gdp_nominal = fred.get_series(
        "MKTGDPZAA646NWDB", observation_start="2000-01-01", observation_end="2019-12-31"
    )

    # Synchronize the indices of all the time series by aligning on the 'Date'
    data = pd.DataFrame(
        {
            "Date": interest_rate.index,  # Use the 'Date' from the resampled interest_rate (yearly)
            "Construction Sector": construction_growth.reindex(
                interest_rate.index, method="ffill"
            ).values,
            "Manufacturing Sector": manufacturing_growth.reindex(
                interest_rate.index, method="ffill"
            ).values,
            "Mining Sector": mining_growth.reindex(
                interest_rate.index, method="ffill"
            ).values,
            "Inflation": inflation.reindex(interest_rate.index, method="ffill").values,
            "Real GDP Per Capita": real_gdp.reindex(
                interest_rate.index, method="ffill"
            ).values,
            "Nominal GDP": gdp_nominal.reindex(
                interest_rate.index, method="ffill"
            ).values,
        }
    )

    return data


# T
def transform_data(df):
    # Convert 'Date' to datetime and set it as the index
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    # Interpolate missing values if necessary (e.g., if there are any NaN values)
    df = df.interpolate(method="linear")

    # Calculate Year-over-Year (YoY) percentage change for relevant columns

    df["Manufacturing YoY Change"] = df["Manufacturing Sector"].pct_change() * 100
    df["Mining YoY Change"] = df["Mining Sector"].pct_change() * 100
    df["Construction YoY Change"] = df["Construction Sector"].pct_change() * 100
    df["Inflation YoY Change"] = df["Inflation"].pct_change() * 100
    df["Real GDP Per Capita YoY Change"] = df["Real GDP Per Capita"].pct_change() * 100
    df["Nominal GDP YoY Change"] = df["Nominal GDP"].pct_change() * 100

    # Optional: Drop rows with missing values after percentage change (if needed)
    df = df.dropna()

    return df


# L
def load_data(df, output_path="Download/sa_economic_data.csv"):
    # Save data as CSV
    df.to_csv(output_path, index=True)
    # Print
    print(f"Data successfully saved to {output_path}")


# ETL Execution
if __name__ == "__main__":
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)
    print("ETL pipeline executed successfully!")
