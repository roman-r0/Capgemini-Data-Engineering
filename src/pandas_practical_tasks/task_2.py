from pathlib import Path

import pandas as pd


def print_grouped_data(df: pd.DataFrame, message: str | None = None):
    if message:
        print(message)
    print(df.to_string())


def rank_neighborhoods(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=False)
    neighborhood_summary = (
        df.groupby("neighbourhood_group")
        .agg(total_listings=("index", "count"), average_price=("price", "mean"))
        .reset_index()
    )

    # Sort by total number of listings in descending order, then by average price in ascending order
    neighborhood_ranking = neighborhood_summary.sort_values(
        by=["total_listings", "average_price"], ascending=[False, True]
    )

    return neighborhood_ranking


if __name__ == "__main__":
    root_folder = Path(__file__).parent.parent.parent
    input_file_path = root_folder / "data" / "cleaned_airbnb_data.csv"
    output_file_path = root_folder / "data" / "aggregated_airbnb_data.csv"
    dataset = pd.read_csv(input_file_path)

    dataset = dataset.loc[
        (dataset["price"] > 100) & (dataset["number_of_reviews"] > 10)
    ]

    print(dataset.iloc[1].to_string())
    print(dataset.loc[dataset["neighbourhood_group"] == "Manhattan"].info())

    print("\nFiltered dataset by price and number of reviews:")
    print(dataset.info())

    print("\nFiltered dataset by columns:")
    dataset = dataset.loc[
        :,
        [
            "neighbourhood_group",
            "price",
            "minimum_nights",
            "number_of_reviews",
            "price_category",
            "availability_365",
        ],
    ]
    print(dataset.info())

    print_grouped_data(
        dataset.groupby(by=["neighbourhood_group", "price_category"])[
            ["price", "minimum_nights"]
        ].mean(),
        message="\nGrouped and mean price, minimum_nights:",
    )

    print_grouped_data(
        dataset.groupby(by=["neighbourhood_group", "price_category"])[
            ["number_of_reviews", "availability_365"]
        ].mean(),
        message="\nGrouped and mean number_of_reviews, availability_365:",
    )

    print_grouped_data(
        dataset.sort_values(by=["price", "number_of_reviews"], ascending=[False, True]),
        message="\nSorted by price and number_of_reviews",
    )

    dataset = rank_neighborhoods(dataset)
    print_grouped_data(df=dataset, message="\nRanked by neighbourhood:")

    dataset.to_csv(output_file_path, index=False)
