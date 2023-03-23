import json
import os
from zipfile import ZipFile

import pytest

from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.series import Series
from sail_data_layer.series_data_model import SeriesDataModel
from sail_data_layer.tabular_dataset import TabularDataset
from sail_data_layer.test.tools_data_test import ToolsDataTest
from sail_data_layer.validation import validate, validate_data_content_zip


@pytest.mark.active
def test_validate_csvv1():
    """
    This test our ability to convert a longitudinal dataset to a data frame
    """
    path_dir_dataset_prepared = ToolsDataTest.get_path_dir_dataset_prepared()
    path_dir_dataset = os.path.join(path_dir_dataset_prepared, "c75f663e-d9ee-4f1c-9458-79e92d1c126a")
    is_valid, list_problem = validate(path_dir_dataset)
    assert is_valid
    assert len(list_problem) == 0


@pytest.mark.active
def test_validate_data_content_zip_csvv1():
    """
    This test our ability to convert a longitudinal dataset to a data frame
    """
    path_dir_dataset_prepared = ToolsDataTest.get_path_dir_dataset_prepared()
    path_dir_dataset = os.path.join(path_dir_dataset_prepared, "c75f663e-d9ee-4f1c-9458-79e92d1c126a")
    path_file_data_model_zip = os.path.join(path_dir_dataset, "data_model.zip")
    path_file_data_content_zip = os.path.join(path_dir_dataset, "data_content.zip")

    with ZipFile(path_file_data_model_zip) as archive_data_model:
        data_model_jsondict = json.loads(archive_data_model.read("data_model.json"))
    is_valid, list_problem = validate_data_content_zip(path_file_data_content_zip, data_model_jsondict)
    assert is_valid
    assert len(list_problem) == 0
