from pathlib import Path

import pandas as pd


def print_dataframe_info(df: pd.DataFrame, message: str | None):
    if message:
        print(message)

    print(df.info())


def columns_with_missing_values(df: pd.DataFrame) -> pd.Series:
    missing_counts = df.isnull().sum()

    return missing_counts[missing_counts > 0]


def handle_missing_values_for_df(df: pd.DataFrame) -> pd.DataFrame:
    for column in ("name", "host_name"):
        df.loc[df[column].isnull(), column] = "Unknown"

    df.loc[df["last_review"].isnull(), "last_review"] = pd.NaT

    return df


def categorize_by_minimum_nights(minimum_nights: int) -> str:
    if minimum_nights <= 3:
        return "short-term"
    elif 14 > minimum_nights > 3:
        return "medium-term"
    else:
        return "long-term"


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    bins = [-float("inf"), 100, 300, float("inf")]
    labels = ["Low", "Middle", "High"]
    df["price_category"] = pd.cut(df["price"], bins=bins, labels=labels, right=False)

    df["length_of_stay_category"] = df["minimum_nights"].apply(
        categorize_by_minimum_nights
    )

    return df


def get_invalid_rows_by_price(df: pd.DataFrame) -> pd.Series:
    return df[~(df["price"].isnull()) & (df["price"] <= 0)].index


if __name__ == "__main__":
    root_folder = Path(__file__).parent.parent.parent
    data_folder = root_folder / "data"

    file_to_load = data_folder / "AB_NYC_2019.csv"
    file_to_save = data_folder / "cleaned_airbnb_data.csv"

    input_df = pd.read_csv(file_to_load)

    print_dataframe_info(input_df, "Loaded df:")

    print("\nFirst 5 rows:")
    print(input_df.head(5).to_string())
    print("\nDataframe info:")
    print(input_df.info())

    print("\nColumns with missing values:")
    print(columns_with_missing_values(df=input_df))

    input_df = handle_missing_values_for_df(df=input_df)
    print_dataframe_info(input_df, "\nDataFrame after handling missing values:")

    input_df = transform_data(df=input_df)
    print_dataframe_info(input_df, "\nTransformed DataFrame:")
    print(input_df.sample(5).to_string())

    input_df_columns_with_missing_values = columns_with_missing_values(df=input_df)
    assert "name" not in input_df_columns_with_missing_values
    assert "host_name" not in input_df_columns_with_missing_values

    input_df = input_df.drop(index=get_invalid_rows_by_price(input_df), axis=0)

    print_dataframe_info(input_df, "\nDataFrame with valid price:")

    assert get_invalid_rows_by_price(input_df).empty

    input_df.to_csv(file_to_save, index=False)
