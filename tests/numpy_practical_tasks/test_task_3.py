import numpy as np

from src.numpy_practical_tasks.task_3 import (
    create_array,
    transpose,
    reshape_array,
    split_array_by_axis,
    combine_arrays,
)


def test_create_array__should_create_correct_array():
    assert create_array().shape == (6, 6)


def test_transpose__should_return_transposed_array():
    expected = np.array([[11, 21], [12, 22]], dtype="int8")
    result = transpose(np.array([[11, 12], [21, 22]], dtype="int8"))

    np.testing.assert_array_equal(result, expected)


def test_reshape_array__should_reshape_array():
    shape = (3, 12)
    result = reshape_array(create_array(), shape)

    assert result.shape == shape


def test_split_array_by_axis__should_split_data():
    result = split_array_by_axis(create_array().reshape(4, 9), 9, 1)

    assert len(result) == 9


def test_combine_arrays__should_combine_arrays():
    result = combine_arrays(create_array(), create_array())

    assert result.shape == (12, 6)
