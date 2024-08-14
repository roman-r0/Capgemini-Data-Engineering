import pandas as pd

from src.pandas_practical_tasks.task_2 import print_grouped_data, rank_neighborhoods


def test_print_grouped_data__should_print_data(capsys):
    print_grouped_data(df=pd.DataFrame({"Test": [1, 2]}))

    captured = capsys.readouterr()

    assert captured.out == "   Test\n0     1\n1     2\n"


def test_rank_neighborhoods__should_rank_it_correctly():
    result = rank_neighborhoods(
        df=pd.DataFrame(
            {
                "neighbourhood_group": [
                    "Downtown",
                    "Uptown",
                    "Downtown",
                    "Suburb",
                    "Uptown",
                    "Suburb",
                ],
                "price": [200, 250, 180, 120, 220, 130],
            }
        )
    )

    assert result.to_dict() == {
        "average_price": {0: 190.0, 1: 125.0, 2: 235.0},
        "neighbourhood_group": {0: "Downtown", 1: "Suburb", 2: "Uptown"},
        "total_listings": {0: 2, 1: 2, 2: 2},
    }
