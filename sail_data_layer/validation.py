import json
import os
from typing import List, Tuple

from sail_data_layer.csvv1_dataset_serializer import Csvv1DatasetSerializer
from sail_data_layer.dataset_format import DatasetFormat
from sail_data_layer.tabular_dataset_data_model import TabularDatasetDataModel


def validate(path_dir_dataset: str):
    path_file_header = os.path.join(path_dir_dataset, "dataset_header.json")
    with open(path_file_header) as file:
        dict_header = json.load(file)
    data_set_format = DatasetFormat.parse_str(dict_header["dataset_packaging_format"])
    if data_set_format == DatasetFormat.csvv1:
        serializer = Csvv1DatasetSerializer("")
        data_set = serializer.read_dataset_for_path(path_dir_dataset)
        return data_set.validate()
    elif data_set_format == DatasetFormat.fhirv1:
        raise NotImplementedError()
    else:
        raise ValueError(f"Illegal enum value: {data_set_format}")


def validate_data_content_zip(
    path_file_data_content_zip: str, data_model_jsondict: dict, data_set_format: DatasetFormat = DatasetFormat.csvv1
) -> Tuple[bool, List[str]]:
    if data_set_format == DatasetFormat.csvv1:
        serializer = Csvv1DatasetSerializer("")
        data_model = TabularDatasetDataModel.from_dict(data_model_jsondict)
        data_set = serializer.read_dataset_for_data_content_zip("", "", "", "", path_file_data_content_zip, data_model)
        return data_set.validate()
    elif data_set_format == DatasetFormat.fhirv1:
        raise NotImplementedError()
    else:
        raise ValueError(f"Illegal enum value: {data_set_format}")
