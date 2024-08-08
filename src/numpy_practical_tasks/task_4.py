import typing
from pathlib import Path

import numpy as np

from src.numpy_practical_tasks.utils import print_array


def create_array():
    return np.random.randint(low=0, high=100, size=(10, 10), dtype="int8")


def save_array(
    arr: np.array,
    file_format: typing.Literal["csv", "txt", "npy", "npz"],
    file_path: Path | str | None = None,
) -> Path:
    file_path = (
        Path(file_path)
        if file_format
        else Path(__file__).parent / f"saved_array.{file_format}"
    )
    file_path.parent.mkdir(exist_ok=True, parents=True)

    match file_format:
        case "csv":
            np.savetxt(file_path, arr, delimiter=",", fmt="%s")
        case "txt":
            np.savetxt(file_path, arr, fmt="%s")
        case "npy":
            np.save(file_path, arr)
        case "npz":
            np.savez(file_path, arr)
        case _:
            raise ValueError(f"Invalid format {file_format}!")

    return file_path


def load_array_from_file(file_path: Path | str) -> np.array:
    file_format = Path(file_path).suffix.lower().replace(".", "")

    match file_format:
        case "csv":
            return np.loadtxt(file_path, delimiter=",")
        case "txt":
            return np.loadtxt(file_path)
        case "npy":
            return np.load(Path(file_path))
        case "npz":
            npz_file = np.load(file_path)
            return npz_file[npz_file.files[0]]
        case _:
            raise ValueError(f"Invalid format {file_format}!")


def summarize_array(arr: np.array, axis: int | None = None) -> np.floating:
    return np.sum(arr, axis=axis)


def mean_array_value(arr: np.array, axis: int | None = None) -> np.floating:
    return np.mean(arr, axis=axis)


def median_array_value(arr: np.array, axis: int | None = None) -> np.floating:
    return np.median(arr, axis=axis)


def std_array_value(arr: np.array, axis: int | None = None) -> np.floating:
    return np.std(arr, axis=axis)


def apply_aggregate_function_by_axis(
    arr: np.array,
    agg_function: typing.Callable[[np.array, int | None], np.array],
    axis: int,
) -> list[np.array]:
    return agg_function(arr, axis)


if __name__ == "__main__":
    initial_array = create_array()
    print_array(initial_array, message="Created array:")

    folder_for_files = Path(__file__).parent / "folder_for_files"
    folder_for_files.mkdir(parents=True, exist_ok=True)

    files_formats = {"txt", "csv", "npy", "npz"}
    save_files_paths = [
        save_array(
            arr=initial_array,
            file_format=file_format,
            file_path=folder_for_files / f"save_array.{file_format}",
        )
        for file_format in files_formats
    ]

    print(f"Saved array to files: {save_files_paths}")

    for file_format in files_formats:
        loaded_array = load_array_from_file(
            folder_for_files / f"save_array.{file_format}"
        )
        assert loaded_array.shape == (10, 10)
        print_array(arr=loaded_array, message=f"Loaded array using {file_format=}")

    print_array(summarize_array(initial_array), "Sum of the array: ")
    print_array(mean_array_value(initial_array), "Mean of the array: ")
    print_array(median_array_value(initial_array), "Median of the array: ")
    print_array(std_array_value(initial_array), "SD of the array: ")

    print_array(
        apply_aggregate_function_by_axis(initial_array, summarize_array, axis=0),
        message="Sum of the array by axis 0 (columns)",
    )
    print_array(
        apply_aggregate_function_by_axis(initial_array, summarize_array, axis=0),
        message="Mean of the array by axis 0 (columns)",
    )
    print_array(
        apply_aggregate_function_by_axis(initial_array, summarize_array, axis=0),
        message="Median of the array by axis 0 (columns)",
    )
    print_array(
        apply_aggregate_function_by_axis(initial_array, summarize_array, axis=0),
        message="SD of the array by axis 0 (columns)",
    )
