import numpy as np

from src.numpy_practical_tasks.utils import print_array


def test_print_array__should_print_only_array(capsys):
    print_array(arr=np.array([1, 2]))
    captured = capsys.readouterr()

    assert captured.out == "[1 2]\n"


def test_print_array__should_print_message_and_array(capsys):
    print_array(arr=np.array([1, 2]), message="test message")
    captured = capsys.readouterr()

    assert captured.out == "test message\n[1 2]\n"
