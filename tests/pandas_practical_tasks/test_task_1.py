import numpy as np
import pandas as pd
import pytest

from src.pandas_practical_tasks.task_1 import (
    print_dataframe_info,
    columns_with_missing_values,
    categorize_by_minimum_nights,
    get_invalid_rows_by_price,
)


def test_print_dataframe_info__should_print_data(capsys):
    print_dataframe_info(df=pd.DataFrame({"test": [1, 2]}), message="Message")
    captured = capsys.readouterr()

    assert captured.out == (
        "Message\n"
        "<class 'pandas.core.frame.DataFrame'>\n"
        "RangeIndex: 2 entries, 0 to 1\n"
        "Data columns (total 1 columns):\n"
        " #   Column  Non-Null Count  Dtype\n"
        "---  ------  --------------  -----\n"
        " 0   test    2 non-null      int64\n"
        "dtypes: int64(1)\n"
        "memory usage: 148.0 bytes\n"
        "None\n"
    )


def test_columns_with_missing_values__should_return_missing_values():
    result = columns_with_missing_values(
        pd.DataFrame({"Column 1": [1, 2, 3], "Column 2": [None, "Value", np.nan]})
    )

    assert result.to_dict() == {"Column 2": 2}


@pytest.mark.parametrize(
    "value,expected", [(3, "short-term"), (10, "medium-term"), (15, "long-term")]
)
def test_categorize_by_minimum_nights__should_categorize_values_correctly(
    value, expected
):
    assert categorize_by_minimum_nights(value) == expected


def test_get_invalid_rows_by_price__should_return_correct_indexes():
    result = get_invalid_rows_by_price(df=pd.DataFrame({"price": [1, 0, -100]}))

    assert result.to_list() == [1, 2]
