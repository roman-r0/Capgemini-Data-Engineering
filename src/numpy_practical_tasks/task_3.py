import numpy as np

from src.numpy_practical_tasks.utils import print_array


def create_array() -> np.array:
    return np.random.randint(0, 100, size=(6, 6))


def transpose(arr: np.array) -> np.array:
    return arr.transpose()


def reshape_array(arr: np.array, new_shape: tuple) -> np.array:
    return arr.reshape(new_shape)


def split_array_by_axis(
    arr: np.array, indices_or_sections: int, axis: int
) -> list[np.array]:
    return np.split(arr, indices_or_sections, axis=axis)


def combine_arrays(*args, axis: int = 0) -> np.array:
    return np.concatenate(args, axis=axis)


if __name__ == "__main__":
    initial_array = create_array()
    assert initial_array.shape == (6, 6)
    print_array(initial_array, message="Created array:")

    transposed_array = transpose(initial_array)
    assert transposed_array.shape == (6, 6)
    print_array(transposed_array, message="Transposed array:")

    reshaped_array = reshape_array(initial_array, (3, 12))
    assert reshaped_array.shape == (3, 12)
    print_array(reshaped_array, message="Reshaped array:")

    split_array = split_array_by_axis(initial_array, 6, 1)
    assert len(split_array) == 6
    print_array(split_array, message="Split array:")

    combined_array = combine_arrays(initial_array, initial_array)
    assert combined_array.shape == (12, 6)
    print_array(combined_array, message="Combined arrays:")
