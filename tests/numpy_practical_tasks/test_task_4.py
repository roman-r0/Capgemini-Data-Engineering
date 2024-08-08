import shutil
from pathlib import Path

import numpy as np
import pytest

from src.numpy_practical_tasks.task_4 import (
    create_array,
    save_array,
    load_array_from_file,
    summarize_array,
    std_array_value,
    median_array_value,
    mean_array_value,
    apply_aggregate_function_by_axis,
)


@pytest.fixture
def temp_folder_for_files_folder():
    temp_folder = Path(__file__).parent / "tmp"
    shutil.rmtree(temp_folder, ignore_errors=True)
    temp_folder.mkdir(exist_ok=True, parents=True)
    yield temp_folder
    shutil.rmtree(temp_folder, ignore_errors=True)


def test_create_array__should_create_array():
    assert create_array().shape == (10, 10)


def test_save_array__should_raise_value_error(temp_folder_for_files_folder):
    input_array = create_array()
    with pytest.raises(ValueError):
        save_array(
            arr=input_array,
            file_path=temp_folder_for_files_folder / "tmp_file",
            file_format="unknown",
        )


@pytest.mark.parametrize(("file_format"), ["csv", "txt", "npy", "npz"])
def test_save_array__should_save_to_csv(file_format, temp_folder_for_files_folder):
    input_array = create_array()
    result = save_array(
        arr=input_array,
        file_path=temp_folder_for_files_folder / f"tmp_file.{file_format}",
        file_format=file_format,
    )
    assert result.exists()


@pytest.mark.parametrize(("file_format"), ["csv", "txt", "npy", "npz"])
def test_load_array_from_file__should_load_file(
    file_format, temp_folder_for_files_folder
):
    input_array = create_array()
    file_path = temp_folder_for_files_folder / f"tmp_file.{file_format}"
    result = save_array(
        arr=input_array,
        file_path=file_path,
        file_format=file_format,
    )

    loaded_array = load_array_from_file(file_path)
    assert loaded_array.shape == (10, 10)


def test_summarize_array__should_work_correctly():
    assert summarize_array(np.array([[1, 2, 3.1], [0, 0.9, 1]])) == 8


def test_mean_array_value__should_work_correctly():
    assert mean_array_value(np.array([[1, 2, 3.1], [100, 0.9, 1]])) == 18


def test_median_array_value__should_work_correctly():
    assert median_array_value(np.array([[1, 2, 3.1], [0, 0.9, 1]])) == 1


def test_std_array_value__should_work_correctly():
    assert round(std_array_value(np.array([[1, 2, 3.1], [0, 0.9, 1]])), 3) == 0.979


def test_apply_aggregate_function_by_axis__should_work_correctly():
    np.testing.assert_array_almost_equal(
        apply_aggregate_function_by_axis(
            arr=np.array([[1, 2, 3.1], [0, 0.9, 1]]),
            agg_function=summarize_array,
            axis=0,
        ),
        np.array([1, 2.9, 4.1]),
    )
