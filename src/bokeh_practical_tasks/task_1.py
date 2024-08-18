from pathlib import Path

import pandas as pd
from bokeh.layouts import column
from bokeh.plotting import figure, show, output_file
from bokeh.palettes import Category10
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    FactorRange,
    CustomJS,
    Select,
    FixedTicker,
)


def _categorize_age_group(age_value: float) -> str:
    if age_value < 18:
        return "Child"
    elif 18 <= age_value < 35:
        return "Young Adult"
    elif 35 <= age_value < 60:
        return "Adult"
    else:
        return "Senior"


def prepare_dataset(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.dropna(subset=["Age"], inplace=True)
    dataset["Cabin"].fillna("unknown", inplace=True)
    dataset["Embarked"].fillna("unknown", inplace=True)

    dataset["AgeGroup"] = dataset["Age"].apply(_categorize_age_group)

    return dataset


def create_survival_rate_dataset_by_age_group(dataset: pd.DataFrame) -> pd.DataFrame:
    new_dataset = dataset.groupby(["AgeGroup"])["Survived"].mean().reset_index()
    new_dataset.columns = ["AgeGroup", "SurvivalRate"]
    new_dataset["SurvivalRate"] *= 100

    return new_dataset


def create_survival_rate_dataset_by_class_and_gender(
    dataset: pd.DataFrame,
) -> pd.DataFrame:
    new_dataset = dataset.groupby(["Pclass", "Sex"])["Survived"].mean().reset_index()
    new_dataset.columns = ["Pclass", "Sex", "SurvivalRate"]
    new_dataset["SurvivalRate"] *= 100

    return new_dataset


def create_age_group_survival_visualization(
    dataset: pd.DataFrame, output_file_path: Path
):
    dataset = dataset.copy()
    output_file(output_file_path)

    colors = Category10[len(dataset["AgeGroup"].unique())]
    dataset["Color"] = [colors[i % len(colors)] for i in range(len(dataset))]

    source = ColumnDataSource(dataset)

    p = figure(
        x_range=dataset["AgeGroup"],
        title="Survival Rates by Age Group",
        toolbar_location=None,
        tools="",
        x_axis_label="Age Group",
        y_axis_label="Survival Rate (%)",
        y_range=(0, 100),
    )

    p.vbar(
        x="AgeGroup",
        top="SurvivalRate",
        width=0.9,
        source=source,
        color="Color",
        legend_field="AgeGroup",
    )

    hover = HoverTool()
    hover.tooltips = [
        ("Age Group", "@{AgeGroup}"),
        ("Survival Rate", "@{SurvivalRate}"),
    ]
    p.add_tools(hover)

    p.xgrid.grid_line_color = None
    p.y_range.start = 0
    p.y_range.end = 100
    p.xaxis.major_label_orientation = 1
    p.outline_line_color = None
    p.legend.orientation = "horizontal"
    p.legend.location = "top_center"

    show(p)


def create_class_and_gender_visualization(
    dataset: pd.DataFrame, output_file_path: Path
):
    dataset = dataset.copy()

    class_sex = [
        (str(pclass), sex)
        for pclass in dataset["Pclass"].unique()
        for sex in dataset["Sex"].unique()
    ]
    dataset["class_sex"] = class_sex
    dataset["color"] = dataset["Sex"].apply(
        lambda x: "#084594" if x == "male" else "#2171b5"
    )

    source = ColumnDataSource(dataset)

    p = figure(
        x_range=FactorRange(*class_sex),
        title="Class and Gender",
        toolbar_location=None,
        tools="",
    )

    p.vbar(
        x="class_sex",
        top="SurvivalRate",
        width=0.8,
        source=source,
        legend_field="Sex",
        line_color="white",
        color="color",
    )

    hover = HoverTool()
    hover.tooltips = [
        ("Class", "@Pclass"),
        ("Gender", "@Sex"),
        ("Survival Rate", "@SurvivalRate{0.00}%"),
    ]
    p.add_tools(hover)

    p.xaxis.major_label_orientation = 1
    p.y_range.start = 0
    p.y_range.end = 100
    p.xaxis.axis_label = "Class and Gender"
    p.yaxis.axis_label = "Survival Rate (%)"
    p.legend.title = "Gender"

    class_filter = Select(
        title="Filter by Class",
        value="All",
        options=["All"] + [str(pclass) for pclass in dataset["Pclass"].unique()],
    )
    gender_filter = Select(
        title="Filter by Gender", value="All", options=["All", "male", "female"]
    )

    callback = CustomJS(
        args=dict(
            source=source, class_filter=class_filter, gender_filter=gender_filter
        ),
        code="""
        var data = source.data;
        var pclass = class_filter.value;
        var gender = gender_filter.value;

        var x = data['class_sex'];
        var survivalRate = data['SurvivalRate'];
        var color = data['color'];

        // Iterate over all data points and update visibility based on filters
        for (var i = 0; i < x.length; i++) {
            var show = true;

            // Extract class and gender from the 'x' values
            var class_val = x[i][0];
            var gender_val = x[i][1];

            // Filter by class
            if (pclass !== 'All' && class_val !== pclass) {
                show = false;
            }

            // Filter by gender
            if (gender !== 'All' && gender_val !== gender) {
                show = false;
            }

            // Update visibility
            color[i] = show ? (gender_val === 'male' ? "#084594" : "#2171b5") : "rgba(255,255,255,0)";
        }

        source.change.emit();
        """,
    )

    class_filter.js_on_change("value", callback)
    gender_filter.js_on_change("value", callback)

    layout = column(class_filter, gender_filter, p)

    output_file(output_file_path)
    show(layout)


def create_fare_vs_survival(dataset: pd.DataFrame, output_file_path: Path):
    dataset["SurvivalStatusStr"] = dataset["Survived"].map(
        {1: "Survived", 0: "Not Survived"}
    )

    pclass_colors = {1: Category10[3][0], 2: Category10[3][1], 3: Category10[3][2]}
    dataset["color"] = dataset["Pclass"].map(pclass_colors)

    source = ColumnDataSource(dataset)

    p = figure(
        title="Fare vs. Survival",
        x_axis_label="Fare",
        y_axis_label="Survival Status",
        tools="pan,wheel_zoom,box_zoom,reset",
    )

    p.scatter(
        x="Fare",
        y="Survived",
        source=source,
        color="color",
        size=10,
        legend_field="Pclass",
        fill_alpha=0.6,
    )

    hover = HoverTool()
    hover.tooltips = [
        ("Fare", "@Fare"),
        ("Survival Status", "@SurvivalStatusStr"),
        ("Class", "@Pclass"),
    ]
    p.add_tools(hover)

    p.legend.title = "Class"
    p.yaxis.ticker = FixedTicker(ticks=[0, 1])
    p.yaxis.major_label_overrides = {0: "Not Survived", 1: "Survived"}

    output_file(output_file_path)
    show(p)


if __name__ == "__main__":
    current_folder = Path(__file__).parent
    input_file_path = current_folder.parent.parent / "data/Titanic-Dataset.csv"

    dataset = pd.read_csv(input_file_path)

    dataset = prepare_dataset(dataset)
    survival_rate_dataset_by_age_group = create_survival_rate_dataset_by_age_group(
        dataset
    )
    survival_rate_dataset_by_class_and_gender = (
        create_survival_rate_dataset_by_class_and_gender(dataset)
    )

    create_age_group_survival_visualization(
        survival_rate_dataset_by_age_group,
        output_file_path=current_folder / "age_group_survival.html",
    )

    create_class_and_gender_visualization(
        survival_rate_dataset_by_class_and_gender,
        output_file_path=current_folder / "class_and_gender.html",
    )

    create_fare_vs_survival(
        dataset[["Fare", "Survived", "Pclass"]],
        output_file_path=current_folder / "fare_vs_survival.html",
    )
