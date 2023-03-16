import pytest

from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.series import Series
from sail_data_layer.series_data_model import SeriesDataModel
from sail_data_layer.tabular_dataset import TabularDataset


@pytest.mark.active
def test_convert_to_data_frame(tabular_dataset_c4kv_csvv1_210_1: TabularDataset):
    """
    This test our ability to convert a longitudinal dataset to a data frame
    """
    tabular_dataset = tabular_dataset_c4kv_csvv1_210_1
    data_model = tabular_dataset.data_model
    is_valid, list_problem = data_model.validate(tabular_dataset)
    if not is_valid:
        for problem in list_problem:
            print(problem)
    assert is_valid
    # assert len(list_problem) == 0
