from pathlib import Path

import pandas as pd


def print_analysis_results(df: pd.DataFrame | pd.Series, message: str):
    if message:
        print(message)

    if isinstance(df, pd.DataFrame):
        print(df.to_string())
    else:
        print(df)


def classify_by_availability(availability: int) -> str:
    if availability < 50:
        return "Rarely Available"
    elif availability > 200:
        return "Highly Available"
    else:
        return "Occasionally Available"


if __name__ == "__main__":
    root_folder = Path(__file__).parent.parent.parent
    input_file_path = root_folder / "data" / "cleaned_airbnb_data.csv"
    output_file_path = root_folder / "data" / "time_series_airbnb_data.csv"
    dataset = pd.read_csv(input_file_path)

    print_analysis_results(
        pd.pivot_table(
            dataset,
            values="price",
            index="neighbourhood_group",
            columns="room_type",
            aggfunc="mean",
            fill_value=0,
        ),
        message="Analyze Pricing Trends Across Neighborhoods and Room Types:",
    )

    long_format_dataset = pd.melt(
        dataset,
        id_vars=["neighbourhood_group", "room_type"],
        value_vars=["price", "minimum_nights"],
        var_name="metrics",
        value_name="value",
    )
    print_analysis_results(
        long_format_dataset.head(5),
        message="\nPrepare Data for In-Depth Metric Analysis:",
    )

    dataset_with_new_column = dataset.copy()
    dataset_with_new_column["availability_status"] = dataset_with_new_column[
        "availability_365"
    ].apply(classify_by_availability)

    print_analysis_results(
        dataset_with_new_column.head(5), message="\nClassify Listings by Availability:"
    )

    print_analysis_results(df=dataset["price"].mean(), message="\nPrice mean:")
    print_analysis_results(df=dataset["price"].median(), message="\nPrice median:")
    print_analysis_results(
        df=dataset["price"].std(), message="\nPrice standard deviation:"
    )

    print_analysis_results(
        df=dataset["minimum_nights"].mean(), message="\nminimum_nights mean:"
    )
    print_analysis_results(
        df=dataset["minimum_nights"].median(), message="\nminimum_nights median:"
    )
    print_analysis_results(
        df=dataset["minimum_nights"].std(),
        message="\nminimum_nights standard deviation:",
    )

    print_analysis_results(
        df=dataset["number_of_reviews"].mean(), message="\nnumber_of_reviews mean:"
    )
    print_analysis_results(
        df=dataset["number_of_reviews"].median(), message="\nnumber_of_reviews median:"
    )
    print_analysis_results(
        df=dataset["number_of_reviews"].std(),
        message="\nnumber_of_reviews standard deviation:",
    )

    dataset["last_review"] = pd.to_datetime(dataset["last_review"])
    dataset.set_index("last_review", drop=True, inplace=True)
    print(dataset.info())

    time_series_data = dataset.resample("ME").agg(
        {"number_of_reviews": "sum", "price": "mean"}
    )
    print_analysis_results(
        time_series_data,
        message="\nIdentify Monthly Trends:",
    )

    print_analysis_results(
        dataset.resample("ME").agg({"number_of_reviews": "mean", "price": "mean"}),
        message="\nAnalyze Seasonal Patterns:",
    )

    time_series_data.to_csv(output_file_path)
