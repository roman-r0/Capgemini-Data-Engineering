import operator
import typing
from collections import Counter, defaultdict
from collections.abc import Sequence

import numpy as np

from src.numpy_practical_tasks.utils import print_array


def create_array() -> np.array:
    return np.rec.array(
        [
            (1, 1, 1, 10, 100.1, "2024-08-06T12:00:00+0000"),
            (2, 1, 2, 15, 10.5, "2024-08-06T09:15:00+0000"),
            (3, 1, 2, 10, 10.1, "2024-08-07T00:00:00+0000"),
            (4, 1, 3, 5, 1, "2024-08-06T10:19:23+0000"),
            (5, 2, 3, 1, 1, "2024-08-03T11:00:00+0000"),
            (6, 2, 3, 20, 1, "2024-08-02T16:00:00+0000"),
            (7, 3, 3, 1, 1, "2024-08-06T20:00:19+0000"),
            (8, 4, 3, 1, 1, "2024-08-05T00:00:00+0000"),
            (9, 5, 1, 0, 100.1, "2024-08-06T00:00:00+0000"),
            (10, 6, 1, 10, 100.1, "2024-08-01T00:00:00+0000"),
        ],
        dtype=[
            ("transaction_id", "int8"),
            ("user_id", "int8"),
            ("product_id", "int8"),
            ("quantity", "int8"),
            ("price", "float64"),
            ("timestamp", "datetime64[s]"),
        ],
    )


def calculate_total_revenue(arr: np.array) -> np.float64:
    return sum(arr.quantity * arr.price)


def calculate_unique_users(arr: np.array) -> int:
    return len(np.unique(arr.user_id))


def calculate_most_purchased_product(arr: np.array) -> int:
    result_quantity = defaultdict(int)

    for product, quantity in zip(arr.product_id, arr.quantity):
        result_quantity[product] += quantity

    return int(Counter(result_quantity).most_common(1)[0][0])


def cast_float_to_int(arr: np.array) -> np.array:
    if not np.issubdtype(arr.dtype, np.floating):
        raise TypeError(f"Array should has float dtype {arr}!")
    return arr.astype("int64")


def check_dtype_of_each_column(arr: np.array) -> dict:
    return {name: dtype[0].name for name, dtype in arr.dtype.fields.items()}


def create_product_quantity_array(arr: np.array) -> np.array:
    return np.rec.array(
        [arr.product_id, arr.quantity],
        dtype=[
            ("product_id", arr.dtype["product_id"].name),
            ("quantity", arr.dtype["quantity"].name),
        ],
    )


def calculate_transaction_count_per_user(arr: np.array) -> np.array:
    user_transactions = defaultdict(int)

    for user_id, _ in zip(arr.user_id, arr.transaction_id):
        user_transactions[user_id] += 1

    return np.rec.array(
        [
            (user, transaction_count)
            for user, transaction_count in user_transactions.items()
        ],
        dtype=[("user_id", "int8"), ("transaction_count", "int8")],
    )


def create_masked_array_quantity_zero(arr: np.array) -> np.array:
    return arr[arr.quantity > 0]


def increase_price(arr: np.array, percentage_to_increase: float) -> np.array:
    if percentage_to_increase < 0 or percentage_to_increase > 1:
        raise ValueError("Percentage to increase should be between 0 and 1!")
    arr.price += arr.price * percentage_to_increase
    return arr


def filter_transactions_quantity_greater_than_one(arr: np.array) -> np.array:
    return arr[arr.quantity > 1]


def compare_revenue(
    arr: np.array,
    period_one: Sequence[np.datetime64, np.datetime64],
    period_two: Sequence[np.datetime64, np.datetime64],
    function_to_compare: typing.Callable[[np.floating, np.floating], bool],
) -> bool:
    revenue_period_one = calculate_total_revenue(
        arr[(arr.timestamp >= period_one[0]) & (arr.timestamp <= period_one[1])]
    )
    revenue_period_two = calculate_total_revenue(
        arr[(arr.timestamp >= period_two[0]) & (arr.timestamp <= period_two[1])]
    )

    result = function_to_compare(revenue_period_one, revenue_period_two)
    if not isinstance(result, np.bool):
        raise TypeError(f"Can't compare two revenues, invalid {function_to_compare=}")
    return bool(result)


def get_user_transactions(arr: np.array, user_id: int) -> np.array:
    return arr[arr.user_id == user_id].transaction_id


def filter_array_by_date_range(
    arr: np.array, period: Sequence[np.datetime64, np.datetime64]
) -> np.array:
    return arr[
        (arr.timestamp >= period[0]) & (arr.timestamp <= period[1])
    ].transaction_id


def get_top_n_products_by_revenue(arr: np.array, top: int) -> np.array:
    revenue = arr.price * arr.quantity
    product_ids = np.unique(arr.product_id)

    total_revenues = np.zeros_like(product_ids, dtype=float)

    for i, product_id in enumerate(product_ids):
        total_revenues[i] = np.sum(revenue[arr.product_id == product_id])

    top = min(top, len(total_revenues))
    return arr[np.argsort(total_revenues)[-top:][::-1]].product_id


if __name__ == "__main__":
    initial_array = create_array()
    print_array(initial_array, message="Initial array:")
    print_array(calculate_total_revenue(initial_array), "Total revenue:")
    print_array(calculate_unique_users(initial_array), message="Unique users:")
    print_array(
        calculate_most_purchased_product(initial_array),
        message="Most purchased product:",
    )
    print_array(cast_float_to_int(initial_array.price), message="Cast float to int:")
    print_array(check_dtype_of_each_column(initial_array), message="Check dtype:")
    print_array(
        create_product_quantity_array(initial_array), message="Create product quantity:"
    )
    print_array(
        calculate_transaction_count_per_user(initial_array),
        message="User transaction count:",
    )
    print_array(
        create_masked_array_quantity_zero(initial_array), message="Masked array:"
    )
    print_array(increase_price(initial_array, 0.05), message="Increase price:")
    print_array(
        filter_transactions_quantity_greater_than_one(initial_array),
        message="Filter transaction:",
    )
    print_array(
        compare_revenue(
            arr=initial_array,
            period_one=(np.datetime64("2024-08-01"), np.datetime64("2024-08-03")),
            period_two=(np.datetime64("2024-08-04"), np.datetime64("2024-08-07")),
            function_to_compare=operator.le,
        ),
        message="Revenue comparison:",
    )
    print_array(get_user_transactions(initial_array, 2), message="User transactions:")
    print_array(
        filter_array_by_date_range(
            initial_array,
            period=(np.datetime64("2024-08-01"), np.datetime64("2024-08-03")),
        ),
        message="Date range slicing",
    )
    print_array(
        get_top_n_products_by_revenue(initial_array, 5),
        message="Top 5 products by revenue:",
    )
