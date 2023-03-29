import pytest

from sail_data_layer.aggregator import Aggregator
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.data_type_enum import DataTypeEnum
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.series import Series
from sail_data_layer.series_data_model import SeriesDataModelInterval


@pytest.mark.active
def test_convert_to_data_frame(longitudinal_dataset_r4sep2019_fhirv1_20_1: LongitudinalDataset):
    """
    This test our ability to convert a longitudinal dataset to a data frame
    """
    dataset_longitudinal = longitudinal_dataset_r4sep2019_fhirv1_20_1

    # Arrange
    data_frame_name = "data_frame_0"
    list_aggregator = []
    list_aggregator.append(
        Aggregator(
            "Observation:Body Mass Index",
            Aggregator.AggregatorIntervalMean,
            SeriesDataModelInterval("bmi_mean", unit="kg/m2"),
        )
    )

    list_aggregator.append(
        Aggregator(
            "Observation:Body Mass Index",
            Aggregator.AggregatorIntervalFirstOccurance,
            SeriesDataModelInterval("bmi_first", unit="kg/m2"),
        )
    )

    list_aggregator.append(
        Aggregator(
            "Observation:Body Mass Index",
            Aggregator.AggregatorIntervalLastOccurance,
            SeriesDataModelInterval("bmi_last", unit="kg/m2"),
        )
    )

    # Act
    # TODO
    data_frame = dataset_longitudinal.convert_to_data_frame(data_frame_name, list_aggregator)

    name_series_1 = data_frame.list_series_name[1]
    name_series_2 = data_frame.list_series_name[2]
    data_model_series = data_frame[name_series_1].data_model_series

    # Assert
    assert isinstance(data_frame, DataFrame)
    assert isinstance(data_frame[name_series_1], Series)
    assert data_model_series.data_type == DataTypeEnum.Interval
    assert data_model_series.unit == "kg/m2"
