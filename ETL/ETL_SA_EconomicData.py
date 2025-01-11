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

    # sectors
    manufacturing_growth = fred.get_series(
        "ZAFPRMNTO01GPSAM", observation_start="2002-02-01", observation_end="2022-11-01"
    )  # monthly
    mining_growth = fred.get_series(
        "ZAFPIEAMI02GPM", observation_start="2002-02-01", observation_end="2022-11-01"
    )  # monthly
    agriculture_growth = fred.get_series(
        "ZAFCP070200GPM", observation_start="2002-02-01", observation_end="2022-11-01"
    )  # monthly
    all_Shares_growth = fred.get_series(
        "SPASTT01ZAM657N", observation_start="2002-02-01", observation_end="2022-11-01"
    )  # monthly

    monthyly_data = pd.DataFrame(
        {
            "Date": manufacturing_growth.index,
            "Manufacturing Growth": manufacturing_growth.values,
            "Mining Growth": mining_growth.values,
            "Agriculture Growth": agriculture_growth.values,
            "Mining Growth": all_Shares_growth.values,
        }
    )

    ##################

    inflation_rate = fred.get_series(
        "FPCPITOTLZGZAF", observation_start="2000-01-01", observation_end="2023-01-01"
    )  # yearly
    unemployment_rate = fred.get_series(
        "LRUN64TTZAA156S", observation_start="2000-01-01", observation_end="2023-01-01"
    )  # yearly
    real_gdp_perCapita = fred.get_series(
        "ZAFNGDPRPCPCPPPT", observation_start="2000-01-01", observation_end="2023-01-01"
    )  # yearly
    gdp_nominal = fred.get_series(
        "MKTGDPZAA646NWDB", observation_start="2000-01-01", observation_end="2023-01-01"
    )  # yearly
    # DF 1
    yearly_data = pd.DataFrame(
        {
            "Date": inflation_rate.index,
            "Inflation Rate": inflation_rate.values,
            "Unemployment Rate": unemployment_rate.values,
            "Real GDP Per Capita": real_gdp_perCapita.values,
            "Nominal GDP": gdp_nominal.values,
        }
    )
    return [monthyly_data, yearly_data]


# T
def transform_data(df):
    # Convert 'Date' to datetime and set it as the index
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    # Interpolate missing values if necessary ()
    df = df.interpolate(method="linear")

    # df = df.dropna()

    return df


# L
def load_data(df, output_path):
    # Save data as CSV
    df.to_csv(output_path, index=True)
    # Print
    print(f"Data successfully saved to {output_path}")


# ETL Process Extraction
if __name__ == "__main__":
    raw_data = extract_data()

    monthly_raw_data = raw_data[0]
    yearly_raw_data = raw_data[1]

    monthly_transformed_data = transform_data(monthly_raw_data)
    yearly_transformed_data = transform_data(yearly_raw_data)

    load_data(monthly_transformed_data, "data/monthly_data.csv")
    load_data(yearly_transformed_data, "data/yearly_data.csv")

    print("ETL pipeline executed successfully!")
