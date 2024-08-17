from pathlib import Path
from sklearn.linear_model import LinearRegression

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


def neighborhood_distribution_of_listings(
    neighbourhood_group_data: pd.Series,
) -> plt.figure():
    neighbourhood_group_counts = neighbourhood_group_data.value_counts()
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = plt.get_cmap("tab10").colors
    bars = ax.bar(
        neighbourhood_group_counts.index,
        neighbourhood_group_counts.values,
        color=colors,
    )

    for bar in bars:
        yval = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2, yval, int(yval), va="bottom", ha="center"
        )

    ax.set_title("Neighborhood Distribution of Listings")
    ax.set_xlabel("Neighborhood Group")
    ax.set_ylabel("Number of Listings")

    ax.set_xticklabels(neighbourhood_group_counts.index, rotation=45)
    plt.tight_layout()

    return fig


def price_distribution_across_neighborhoods(graph_data: pd.DataFrame) -> plt.figure:
    neighbourhood_groups = graph_data["neighbourhood_group"].unique()
    prices_by_group = [
        graph_data[graph_data["neighbourhood_group"] == group]["price"]
        for group in neighbourhood_groups
    ]

    fig, ax = plt.subplots(figsize=(12, 8))
    box = ax.boxplot(
        prices_by_group,
        tick_labels=neighbourhood_groups,
        patch_artist=True,
        showfliers=True,
    )
    cmap = plt.get_cmap("tab10", len(neighbourhood_groups))

    for i, patch in enumerate(box["boxes"]):
        color = cmap(i)
        patch.set_facecolor(color)
        patch.set_edgecolor("black")

    for flier in box["fliers"]:
        flier.set(marker="o", color="red", alpha=0.7)

    ax.set_title("Price Distribution Across Neighborhoods")
    ax.set_xlabel("Neighborhood Group")
    ax.set_ylabel("Price")

    plt.tight_layout()
    return fig


def room_type_vs_availability(graph_data: pd.DataFrame) -> plt.figure:
    grouped_data = (
        graph_data.groupby(["neighbourhood_group", "room_type"])["availability_365"]
        .agg(["mean", "std"])
        .reset_index()
    )
    pivot_data = grouped_data.pivot(
        index="neighbourhood_group", columns="room_type", values=["mean", "std"]
    )
    neighborhoods = pivot_data.index
    room_types = pivot_data.columns.levels[1]

    n_neigh = len(neighborhoods)
    n_rooms = len(room_types)

    fig, ax = plt.subplots(figsize=(12, 8))

    bar_width = 0.8 / n_rooms
    index = np.arange(n_neigh)

    for i, room_type in enumerate(room_types):
        mean_values = pivot_data[("mean", room_type)]
        std_values = pivot_data[("std", room_type)]
        bar_positions = index + i * bar_width

        bars = ax.bar(
            bar_positions,
            mean_values,
            bar_width,
            yerr=std_values,
            label=room_type,
            capsize=5,
        )

        for bar in bars:
            bar.set_edgecolor("black")

    ax.set_xlabel("Neighborhood Group")
    ax.set_ylabel("Average Availability (365 days)")
    ax.set_title("Room Type vs. Availability")
    ax.set_xticks(index + bar_width * (n_rooms - 1) / 2)
    ax.set_xticklabels(neighborhoods, rotation=45)
    ax.legend(title="Room Type")

    ax.yaxis.grid(True)

    plt.tight_layout()

    return fig


def correlation_between_price_and_number_of_reviews(
    graph_data: pd.DataFrame,
) -> plt.figure:
    room_types = graph_data["room_type"].unique()
    colors = plt.get_cmap("tab10").colors

    markers = ["o", "s", "^", "D", "x"]

    fig, ax = plt.subplots(figsize=(12, 8))

    for i, room_type in enumerate(room_types):
        data = graph_data[graph_data["room_type"] == room_type]
        x = data["price"].values.reshape(-1, 1)
        y = data["number_of_reviews"].values

        ax.scatter(
            x,
            y,
            color=colors[i % len(colors)],
            marker=markers[i % len(markers)],
            label=room_type,
        )

        if len(x) > 1:
            model = LinearRegression()
            model.fit(x, y)
            x_range = np.linspace(x.min(), x.max(), 100).reshape(-1, 1)
            y_pred = model.predict(x_range)
            ax.plot(
                x_range,
                y_pred,
                color=colors[i % len(colors)],
                linestyle="--",
                linewidth=2,
            )

    ax.set_title("Correlation Between Price and Number of Reviews")
    ax.set_xlabel("Price")
    ax.set_ylabel("Number of Reviews")
    ax.legend(title="Room Type")

    ax.grid(True)
    plt.tight_layout()

    return fig


def time_series_analysis_of_reviews(
    graph_data: pd.DataFrame,
) -> plt.figure:
    graph_data["last_review"] = pd.to_datetime(
        graph_data["last_review"], errors="coerce"
    )

    df = graph_data.dropna(subset=["last_review", "number_of_reviews"])

    neighborhood_groups = df["neighbourhood_group"].unique()
    colors = plt.get_cmap("tab10").colors

    fig, ax = plt.subplots(figsize=(12, 8))

    for i, neighborhood in enumerate(neighborhood_groups):
        data = df[df["neighbourhood_group"] == neighborhood]
        data = data.sort_values(by="last_review")
        rolling_avg = data["number_of_reviews"].rolling(window=30, min_periods=1).mean()
        ax.plot(
            data["last_review"],
            rolling_avg,
            color=colors[i % len(colors)],
            label=neighborhood,
        )

    ax.set_title("Time Series Analysis of Reviews")
    ax.set_xlabel("Last Review Date")
    ax.set_ylabel("Rolling Average of Number of Reviews")
    ax.legend(title="Neighborhood Group")

    ax.xaxis.set_major_locator(plt.MaxNLocator(10))
    plt.xticks(rotation=45)

    ax.grid(True)

    plt.tight_layout()
    return fig


def price_and_availability_heatmap(
    graph_data: pd.DataFrame,
) -> plt.figure:
    pivot_table = (
        graph_data.groupby("neighbourhood_group")
        .agg({"price": "mean", "availability_365": "mean"})
        .reset_index()
    )

    neighborhoods = pivot_table["neighbourhood_group"]
    price_values = pivot_table["price"]
    availability_values = pivot_table["availability_365"]

    grid = np.zeros((len(neighborhoods), len(neighborhoods)))

    for i, neighborhood in enumerate(neighborhoods):
        for j in range(len(neighborhoods)):
            grid[i, j] = price_values[i] * availability_values[j]

    fig, ax = plt.subplots(figsize=(12, 8))
    cax = ax.imshow(grid, cmap="viridis", interpolation="nearest")

    cbar = fig.colorbar(cax, ax=ax, orientation="vertical")
    cbar.set_label("Intensity")

    ax.set_title("Price and Availability Heatmap")
    ax.set_xlabel("Neighborhoods (Price)")
    ax.set_ylabel("Neighborhoods (Availability_365)")

    ax.set_xticks(np.arange(len(neighborhoods)))
    ax.set_yticks(np.arange(len(neighborhoods)))
    ax.set_xticklabels(neighborhoods, rotation=45, ha="right")
    ax.set_yticklabels(neighborhoods)

    plt.tight_layout()

    return fig


def room_type_and_review_count_analysis(
    graph_data: pd.DataFrame,
) -> plt.figure:
    aggregated_data = (
        graph_data.groupby(["neighbourhood_group", "room_type"])["number_of_reviews"]
        .sum()
        .unstack()
        .fillna(0)
    )

    fig, ax = plt.subplots(figsize=(12, 8))

    room_types = aggregated_data.columns
    colors = plt.get_cmap("tab10").colors

    bottom_values = np.zeros(len(aggregated_data))
    for i, room_type in enumerate(room_types):
        ax.bar(
            aggregated_data.index,
            aggregated_data[room_type],
            bottom=bottom_values,
            color=colors[i % len(colors)],
            label=room_type,
        )
        bottom_values += aggregated_data[room_type]

    ax.set_title("Room Type and Review Count Analysis")
    ax.set_xlabel("Neighborhood Group")
    ax.set_ylabel("Number of Reviews")
    ax.legend(title="Room Type")

    plt.xticks(rotation=45)
    ax.grid(True, axis="y")

    plt.tight_layout()

    return fig


if __name__ == "__main__":
    current_folder = Path(__file__).parent
    data_folder = current_folder.parent.parent / "data"
    file_path = data_folder / "cleaned_airbnb_data.csv"
    dataset = pd.read_csv(file_path)

    result_fig_1 = neighborhood_distribution_of_listings(dataset["neighbourhood_group"])
    result_fig_1.savefig(current_folder / "neighborhood_distribution_of_listings.png")
    result_fig_1.show()
    plt.close(result_fig_1)

    result_fig_2 = price_distribution_across_neighborhoods(dataset)
    result_fig_2.savefig(current_folder / "price_distribution_across_neighborhoods.png")
    result_fig_2.show()
    plt.close(result_fig_2)

    result_fig_3 = room_type_vs_availability(dataset)
    result_fig_3.savefig(current_folder / "room_type_vs_availability.png")
    result_fig_3.show()
    plt.close(result_fig_3)

    result_fig_4 = correlation_between_price_and_number_of_reviews(dataset)
    result_fig_4.savefig(
        current_folder / "correlation_between_price_and_number_of_reviews.png"
    )
    result_fig_4.show()
    plt.close(result_fig_4)

    result_fig_5 = time_series_analysis_of_reviews(dataset)
    result_fig_5.savefig(current_folder / "time_series_analysis_of_reviews.png")
    result_fig_5.show()
    plt.close(result_fig_5)

    result_fig_6 = price_and_availability_heatmap(dataset)
    result_fig_6.savefig(current_folder / "price_and_availability_heatmap.png")
    result_fig_6.show()
    plt.close(result_fig_6)

    result_fig_7 = room_type_and_review_count_analysis(dataset)
    result_fig_7.savefig(current_folder / "room_type_and_review_count_analysis.png")
    result_fig_7.show()
    plt.close(result_fig_7)
