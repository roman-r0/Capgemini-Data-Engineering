import operator

import numpy as np
import pytest

from src.numpy_practical_tasks.task_2 import (
    create_array,
    calculate_total_revenue,
    calculate_unique_users,
    calculate_most_purchased_product,
    cast_float_to_int,
    check_dtype_of_each_column,
    create_product_quantity_array,
    calculate_transaction_count_per_user,
    create_masked_array_quantity_zero,
    increase_price,
    filter_transactions_quantity_greater_than_one,
    compare_revenue,
    get_user_transactions,
    filter_array_by_date_range,
    get_top_n_products_by_revenue,
)


def test_create_array__return_array():
    arr = create_array()
    necessary_fields = {
        "transaction_id",
        "user_id",
        "product_id",
        "quantity",
        "price",
        "timestamp",
    }

    assert arr.size == 10
    assert set(arr.dtype.names) == necessary_fields


def test_calculate_total_revenue__should_return_correct_value():
    assert calculate_total_revenue(create_array()) == np.float64(2288.5)


def test_calculate_unique_users__should_return_correct_value():
    assert calculate_unique_users(create_array()) == 6


def test_calculate_most_purchased_product():
    assert calculate_most_purchased_product(create_array()) == 3


def test_cast_float_to_int__should_work_correctly():
    result = cast_float_to_int(create_array().price)

    assert np.issubdtype(result.dtype, np.integer)


def test_check_dtype_of_each_column__should_work_correctly():
    expected = {
        "price": "float64",
        "product_id": "int8",
        "quantity": "int8",
        "timestamp": "datetime64[s]",
        "transaction_id": "int8",
        "user_id": "int8",
    }
    assert check_dtype_of_each_column(create_array()) == expected


def test_create_product_quantity_array__should_create_array():
    input_array = create_array()
    result_arr = create_product_quantity_array(input_array)

    assert result_arr.size == input_array.size
    assert set(result_arr.dtype.names) == {"product_id", "quantity"}


def test_calculate_transaction_count_per_user__should_return_correct_array():
    input_array = create_array()
    result_array = calculate_transaction_count_per_user(input_array)
    expected = np.rec.array(
        [(1, 4), (2, 2), (3, 1), (4, 1), (5, 1), (6, 1)],
        dtype=result_array.dtype,
    )
    np.testing.assert_array_equal(result_array, expected)


def test_create_masked_array_quantity_zero__should_filter_array():
    input_array = create_array()
    result_array = create_masked_array_quantity_zero(input_array)
    assert result_array.size == 9


def test_increase_price__should_increase_price():
    input_array = create_array()

    result = increase_price(input_array, 0.05)

    np.testing.assert_array_almost_equal(
        result.price,
        np.array(
            [105.105, 11.025, 10.605, 1.05, 1.05, 1.05, 1.05, 1.05, 105.105, 105.105],
            dtype=result.price.dtype,
        ),
    )


def test_filter_transactions_quantity_greater_than_one__should_return_filtered_array():
    result = filter_transactions_quantity_greater_than_one(create_array())

    assert result.size == 6


def test_compare_revenue__should_work_correctly():
    input_array = create_array()

    result = compare_revenue(
        arr=input_array,
        period_one=(np.datetime64("2024-08-01"), np.datetime64("2024-08-03")),
        period_two=(np.datetime64("2024-08-04"), np.datetime64("2024-08-07")),
        function_to_compare=operator.le,
    )

    assert result is True


def test_compare_revenue__should_raise_type_error():
    input_array = create_array()

    with pytest.raises(TypeError):
        compare_revenue(
            arr=input_array,
            period_one=(np.datetime64("2024-08-01"), np.datetime64("2024-08-03")),
            period_two=(np.datetime64("2024-08-04"), np.datetime64("2024-08-07")),
            function_to_compare=operator.add,
        )


def test_get_user_transactions__should_return_filtered_transactions():
    result = get_user_transactions(create_array(), user_id=1)

    np.testing.assert_array_equal(result, np.array([1, 2, 3, 4], dtype=result.dtype))


def test_filter_array_by_date_range__should_return_filtered_transactions():
    result = filter_array_by_date_range(
        create_array(), (np.datetime64("2024-08-01"), np.datetime64("2024-08-03"))
    )

    np.testing.assert_array_equal(result, np.array([6, 10], dtype=result.dtype))


def test_get_top_n_products_by_revenue__should_return_correct_values():
    result = get_top_n_products_by_revenue(create_array(), 1)

    np.testing.assert_array_equal(result, np.array([1], dtype=result.dtype))
