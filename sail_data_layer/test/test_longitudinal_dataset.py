import pytest

from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.series import Series
from sail_data_layer.series_data_model import SeriesDataModel


@pytest.mark.active
def test_convert_to_data_frame(longitudinal_dataset_r4sep2019_20_1: LongitudinalDataset):
    """
    This test our ability to convert a longitudinal dataset to a data frame
    """
    dataset_longitudinal = longitudinal_dataset_r4sep2019_20_1

    # Arrange
    data_frame_name = "data_frame_0"

    data_model_data_frame = DataFrameDataModel(data_frame_name)
    data_model_data_frame.add_data_model_series(
        SeriesDataModel.create_numerical(
            series_name="bmi_mean",
            measurement_source_name="Observation:Body Mass Index",
            type_agregator=SeriesDataModel.AgregatorIntervalMean,
            unit="kg/m2",
        )
    )
    data_model_data_frame.add_data_model_series(
        SeriesDataModel.create_numerical(
            series_name="bmi_first",
            measurement_source_name="Observation:Body Mass Index",
            type_agregator=SeriesDataModel.AgregatorIntervalFirstOccurance,
            unit="kg/m2",
        )
    )

    data_model_data_frame.add_data_model_series(
        SeriesDataModel.create_numerical(
            series_name="bmi_last",
            measurement_source_name="Observation:Body Mass Index",
            type_agregator=SeriesDataModel.AgregatorIntervalLastOccurance,
            unit="kg/m2",
        )
    )

    # Act
    # TODO
    data_frame = longitudinal_dataset_r4sep2019_20_1.convert_to_data_frame(data_model_data_frame)

    name_series_1 = data_frame.list_series_name[1]
    name_series_2 = data_frame.list_series_name[2]
    data_model_series = data_frame[name_series_1].data_model_series

    # Assert
    assert isinstance(data_frame, DataFrame)
    assert isinstance(data_frame[name_series_1], Series)
    assert data_model_series.type_data_level == SeriesDataModel.DataLevelInterval
    assert data_model_series.unit == "kg/m2"
