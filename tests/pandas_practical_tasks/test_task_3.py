import pandas as pd

from src.pandas_practical_tasks.task_3 import print_analysis_results


def test_print_analysis_results__should_print_data(capsys):
    print_analysis_results(df=pd.DataFrame({"test": [1, 2]}), message="Message")
    captured = capsys.readouterr()

    assert captured.out == "Message\n   test\n0     1\n1     2\n"
