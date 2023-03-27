import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas
from pandas.api.types import is_numeric_dtype, is_string_dtype

from sail_data_layer.data_federation_packager import DataFederationPackager
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.fhirv1_dataset_serializer import Fhirv1DatasetSerializer
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.series import Series
from sail_data_layer.series_data_model import (
    SeriesDataModel,
    SeriesDataModelCategorical,
    SeriesDataModelInterval,
    SeriesDataModelUnique,
)


class ToolsDataTest:
    """Tools to interact with federated data"""

    # @staticmethod
    # def from_numpy(dataset_id, array: np.ndarray, list_name_column=None) -> np.ndarray:
    #     data_frame = pandas.DataFrame(array)
    #     if list_name_column is not None:
    #         for name_column_source, name_column_target in zip(data_frame.columns, list_name_column):
    #             data_frame.rename(columns={name_column_source: name_column_target}, inplace=True)
    #     data_frame_federated = DataFrameFederatedLocal()
    #     data_frame_federated.dict_dataframe[dataset_id] = data_frame
    #     return data_frame_federated

    @staticmethod
    def get_path_dir_data_federation_unpackaged():
        return os.path.join(
            ToolsDataTest.get_path_dir_environment("PATH_DIR_DATA_SAIL"), "formatted", "data_federation_unpackaged"
        )

    @staticmethod
    def get_path_dir_data_federation_packaged():
        return os.path.join(
            ToolsDataTest.get_path_dir_environment("PATH_DIR_DATA_SAIL"), "formatted", "data_federation_packaged"
        )

    @staticmethod
    def get_path_dir_dataset_prepared():
        return os.path.join(
            ToolsDataTest.get_path_dir_environment("PATH_DIR_DATA_SAIL"), "formatted", "dataset_prepared"
        )

    @staticmethod
    def get_path_dir_environment(environment_variable_name: str, check_exists: bool = True) -> str:
        environment_variable_value = os.environ.get(environment_variable_name)
        if environment_variable_value is None:
            raise RuntimeError(f"No value for environment variable {environment_variable_name}")
        if check_exists:
            if not os.path.isdir(environment_variable_value):
                raise RuntimeError(
                    f"Path {environment_variable_value} for environment variable {environment_variable_name} is not an existing directory"
                )
        return environment_variable_value

    @staticmethod
    def load_path_file_environment(environment_variable_name: str, check_exists: bool = True) -> str:
        environment_variable_value = os.environ.get(environment_variable_name)
        if environment_variable_value is None:
            raise RuntimeError(f"No value for environment variable {environment_variable_name}")
        if check_exists:
            if not os.path.isfile(environment_variable_value):
                raise RuntimeError(
                    f"Path {environment_variable_value} for environment variable {environment_variable_name} is not an existing directory"
                )
        return environment_variable_value

    @staticmethod
    def from_csv(dict_csv: Dict[str, str]) -> DataFrame:
        data_model_data_frame = DataFrameDataModel("data_frame_0")
        # TODO do this for multy csv as well
        data_frame_pandas = pandas.read_csv(list(dict_csv.values())[0])
        list_series_name = data_frame_pandas.columns
        for series_name in list_series_name:
            if is_numeric_dtype(data_frame_pandas[series_name]):
                data_model_series = SeriesDataModelInterval(series_name)

            elif is_string_dtype(data_frame_pandas[series_name]):
                list_value = pandas.unique(data_frame_pandas[series_name])
                if len(list_value) == len(data_frame_pandas[series_name]):
                    data_model_series = SeriesDataModelUnique(series_name)
                else:
                    data_model_series = SeriesDataModelCategorical(series_name, list_value)

            else:
                raise ValueError("Neither numeric or string dtype")
            data_model_data_frame.add_data_model_series(data_model_series)
        list_reference = []

        raise NotImplementedError()

    @staticmethod
    def read_for_path_file(path_file_data_federation: str) -> LongitudinalDataset:
        # TODO call safe function via RPC ReadDatasetFhirv1Precompute
        packager = DataFederationPackager()
        packager.prepare_data_federation(path_file_data_federation)
        dict_dataset_name_to_dataset_id = packager.get_dict_dataset_name_to_dataset_id(path_file_data_federation)
        data_model_longitudinal = {}
        serializer = Fhirv1DatasetSerializer()

        raise NotImplementedError()
