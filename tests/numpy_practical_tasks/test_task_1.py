import numpy as np

from src.numpy_practical_tasks.task_1 import (
    array_creation_1,
    array_creation_2,
    array_indexing_and_slicing_1,
    array_indexing_and_slicing_2,
    basic_arithmetic_1,
    basic_arithmetic_2,
)


def test_array_creation_1__should_create_array_correctly():
    np.testing.assert_array_equal(array_creation_1(), np.array(range(1, 11)))


def test_array_creation_2__should_create_array_correctly():
    np.testing.assert_array_equal(
        array_creation_2(), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    )


def test_array_indexing_and_slicing_1__should_return_correct_value():
    assert array_indexing_and_slicing_1() == 3


def test_array_indexing_and_slicing_2__should_return_correct_value():
    np.testing.assert_array_equal(
        array_indexing_and_slicing_2(), np.array([[1, 2], [4, 5]])
    )


def test_basic_arithmetic_1__should_print_correct_value(capsys):
    basic_arithmetic_1()
    captured = capsys.readouterr()

    assert captured.out == "[ 6  7  8  9 10 11 12 13 14 15]\n"


def test_basic_arithmetic_2__should_print_correct_value(capsys):
    basic_arithmetic_2()
    captured = capsys.readouterr()

    assert captured.out == "[[ 2  4  6]\n [ 8 10 12]\n [14 16 18]]\n"
