import numpy as np


def print_array(arr: np.array, message: str | None = None):
    if message:
        print(message)
    print(arr)
