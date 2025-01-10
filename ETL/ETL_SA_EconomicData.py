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

    # Monthly series
    construction_growth = construction_growth.resample("Y").mean()
    manufacturing_growth = manufacturing_growth.resample("Y").mean()
    mining_growth = mining_growth.resample("Y").mean()
    interest_rate = interest_rate.resample("Y").mean()

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

    # Create DataFrame
    data = {
        "Date": interest_rate.index,
        "Construction Sector": construction_growth.values,
        "Manufacturing Sector": manufacturing_growth.values,
        "Mining Sector": mining_growth.values,
        "Inflation": inflation.values,
        "Real GDP Per Capita": real_gdp.values,
        "Nominal GDP": gdp_nominal.values,
    }
    return pd.DataFrame(data)


# T
def transform_data(df):
    # Convert 'Date' to datetime and set as index and interpolate missing values
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    df = df.interpolate(method="linear")

    # Example of adding percentage change columns
    df["Interest Rate MoM Change"] = df["Interest Rate"].pct_change() * 100
    df["Manufacturing MoM Change"] = df["Manufacturing"].pct_change() * 100
    df["Mining MoM Change"] = df["Mining"].pct_change() * 100

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
