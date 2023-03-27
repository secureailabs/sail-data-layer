import os

import gdown
import pytest

from sail_data_layer.csvv1_dataset_serializer import Csvv1DatasetSerializer
from sail_data_layer.data_federation_packager import DataFederationPackager
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.fhirv1_dataset_serializer import Fhirv1DatasetSerializer
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.tabular_dataset import TabularDataset
from sail_data_layer.test.tools_data_test import ToolsDataTest


def prepare_test_data():
    list_name_data_federation_test = []
    list_name_data_federation_test.append("r4sep2019_fhirv1_20_1")
    # list_name_data_federation_test.append("gdc_csvv1_941_4")
    # list_name_data_federation_test.append("c4kv_csvv1_210_3")
    list_name_data_federation_test.append("c4kv_csvv1_210_1")
    # download data_federations if needed

    # prepare
    packager = DataFederationPackager()
    path_dir_data_federation_packaged = ToolsDataTest.get_path_dir_data_federation_packaged()
    for name_data_federation in list_name_data_federation_test:
        path_file_data_federation = os.path.join(path_dir_data_federation_packaged, name_data_federation + ".zip")
        packager.prepare_data_federation(path_file_data_federation)


def download_test_data():
    # data federations are maintained in google drive,
    # link are accesable at the sail user-group level
    dict_datafederation_packaged = {}
    dict_datafederation_packaged[
        "r4sep2019_fhirv1_20_1"
    ] = "https://drive.google.com/file/d/10Ur8U4dc3dliucmSiYZ9JCEuI9WCRxyR"
    dict_datafederation_packaged[
        "c4kv_csvv1_210_1"
    ] = "https://drive.google.com/file/d/10TwCDXmwWfLQhEAuVP7rFIYb9z-HL65W"
    path_dir_data_federation_packaged = ToolsDataTest.get_path_dir_data_federation_packaged()
    os.makedirs(path_dir_data_federation_packaged, exist_ok=True)
    for name_data_federation, url in dict_datafederation_packaged.items():
        path_file_data_federation = os.path.join(path_dir_data_federation_packaged, name_data_federation + ".zip")
        if not os.path.isfile(path_file_data_federation):
            gdown.download(url, path_file_data_federation, quiet=False)


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    prepare_test_data()


def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """


@pytest.fixture
def tabular_dataset_c4kv_csvv1_210_1() -> TabularDataset:
    serializer = Csvv1DatasetSerializer()
    path_dir_dataset_prepared = ToolsDataTest.get_path_dir_dataset_prepared()
    header = ToolsDataTest.get_data_federation_header("c4kv_csvv1_210_1")
    dataset_id = header["list_dataset_header"][0]["dataset_id"]
    path_file_dataset = os.path.join(path_dir_dataset_prepared, dataset_id)
    return serializer.read_dataset_for_path(path_file_dataset)


@pytest.fixture
def longitudinal_dataset_r4sep2019_20_1() -> LongitudinalDataset:
    serializer = Fhirv1DatasetSerializer()
    path_dir_dataset_prepared = ToolsDataTest.get_path_dir_dataset_prepared()
    header = ToolsDataTest.get_data_federation_header("r4sep2019_20_1")
    dataset_id = header["list_dataset_header"][0]["dataset_id"]
    path_file_dataset = os.path.join(path_dir_dataset_prepared, dataset_id)
    return serializer.read_dataset_for_path(path_file_dataset)


# @pytest.fixture
# def longitudinal_dataset_r4sep2019_1k_3() -> LongitudinalDataset:
#     path_file_data_federation = os.path.join(DATA_PATH, "data_federation_packaged", "r4sep2019_fhirv1_1k_3.zip")
#     return ToolsDataTest.read_for_path_file(path_file_data_federation)


# pytest.fixture
# def tabular_dataset_gdc_941_1(data_path: str) -> DataFrame:
#     list_name_file_csv = ["gdc_csvv1_941_0.csv"]
#     dict_csv = {}
#     for name_file_csv in list_name_file_csv:
#         path_file_csv = os.path.join(data_path, "data_csv_gdc_941_1", name_file_csv)
#         dict_csv[name_file_csv] = path_file_csv
#     return ToolsDataTest.from_csv(dict_csv)
