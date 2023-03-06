import json
import os
import shutil
import tempfile
from typing import Dict
from zipfile import ZIP_DEFLATED, ZipFile

from genericpath import isdir


class DataFederationPackager:
    def __init__(self) -> None:
        path_dir_dataset_store = os.environ.get("PATH_DIR_DATASET")
        if path_dir_dataset_store is None:
            raise Exception("Evironment variable: `PATH_DIR_DATASET` not set")

        self.path_dir_dataset_store = path_dir_dataset_store

    def package_data_federation(
        self,
        path_dir_data_federation_source: str,
        path_file_data_federation_target: str,
    ) -> None:

        if not os.path.isdir(path_dir_data_federation_source):
            raise Exception(f"no data federation  at: {path_dir_data_federation_source}")

        if os.path.isfile(path_file_data_federation_target):
            os.remove(path_file_data_federation_target)

        path_file_data_federation_header = os.path.join(path_dir_data_federation_source, "data_federation_header.json")
        if not os.path.isfile(path_file_data_federation_header):
            raise Exception(f"missing data_federation_header at: {path_file_data_federation_header}")

        with open(path_file_data_federation_header, "r") as file:
            data_federation_header = json.load(file)

        if not self.validate_data_federation_header(data_federation_header):
            raise Exception(f"invalid data_federation_header at: {path_file_data_federation_header}")

        # TODO validate
        path_dir_dataset_root = os.path.join(path_dir_data_federation_source, "dataset")
        if not os.path.isdir(path_dir_dataset_root):
            raise Exception(f"missing dataset_root at: {path_dir_data_federation_source}")

        list_name_dataset_id = os.listdir(path_dir_dataset_root)

        path_dir_dataset_root_temp = tempfile.mkdtemp()
        # package every dataset in the data federation in a temp dir
        for dataset_id in list_name_dataset_id:
            path_dir_dataset = os.path.join(path_dir_dataset_root, dataset_id)
            path_file_dataset = os.path.join(path_dir_dataset_root_temp, dataset_id + ".zip")
            self.package_dataset(path_dir_dataset, path_file_dataset, data_federation_header)

        # zip entire dataset
        with ZipFile(path_file_data_federation_target, "w", ZIP_DEFLATED, compresslevel=9) as archive:
            # add header
            archive.write(path_file_data_federation_header, arcname="data_federation_header.json")
            # add data_set
            for name_file in os.listdir(path_dir_dataset_root_temp):
                path_file_source = os.path.join(path_dir_dataset_root_temp, name_file)
                path_file_target = os.path.join("dataset", name_file)
                archive.write(path_file_source, arcname=path_file_target)

        # remove the temp dir where everything was prepared for packaging
        shutil.rmtree(path_dir_dataset_root_temp)

    def package_dataset(self, path_dir_source: str, path_file_target: str, data_federation_header: Dict) -> None:
        path_file_dataset_header = os.path.join(path_dir_source, "dataset_header.json")
        if not os.path.isfile(path_file_dataset_header):
            raise Exception(f"missing dataset_header at: {path_file_dataset_header}")

        path_dir_data_content = os.path.join(path_dir_source, "data_content")
        if not os.path.isdir(path_dir_data_content):
            raise Exception(f"missing data_content at: {path_dir_data_content}")

        if 0 == len(os.listdir(path_dir_data_content)):
            raise Exception(f"data_content empty at: {path_dir_data_content}")

        path_dir_data_model = os.path.join(path_dir_source, "data_model")
        if not os.path.isdir(path_dir_data_model):
            raise Exception(f"missing data_model at: {path_dir_data_model}")

        if 0 == len(os.listdir(path_dir_data_model)):
            raise Exception(f"data_model empty at: {path_dir_data_model}")

        with open(path_file_dataset_header, "r") as file:
            dataset_header = json.load(file)

        if not self.validate_dataset_header(dataset_header, data_federation_header):
            raise Exception(f"invalid data_federation_header at: {path_file_dataset_header}")

        # TODO validate data_model

        # TODO validate data_content using data_model

        # if the content file exist delete to avoid duplicate addition and such
        path_file_data_content_zip = os.path.join(path_dir_source, "data_content.zip")
        if os.path.isfile(path_file_data_content_zip):
            os.remove(path_file_data_content_zip)

        # zip and compress data_content
        with ZipFile(path_file_data_content_zip, "w", ZIP_DEFLATED, compresslevel=9) as archive:
            for name_file in os.listdir(path_dir_data_content):
                path_file = os.path.join(path_dir_data_content, name_file)
                archive.write(path_file, arcname=name_file)

        # TODO encrypt data_content
        # if the content file exist delete to avoid duplicate addition and such
        path_file_data_model_zip = os.path.join(path_dir_source, "data_model.zip")
        if os.path.isfile(path_file_data_model_zip):
            os.remove(path_file_data_model_zip)

        # zip and compress data_content
        with ZipFile(path_file_data_model_zip, "w", ZIP_DEFLATED, compresslevel=9) as archive:
            for name_file in os.listdir(path_dir_data_model):
                path_file = os.path.join(path_dir_data_model, name_file)
                archive.write(path_file, arcname=name_file)

        # zip entire dataset
        with ZipFile(path_file_target, "w", ZIP_DEFLATED, compresslevel=9) as archive:
            archive.write(path_file_dataset_header, arcname="dataset_header.json")
            archive.write(path_file_data_content_zip, arcname="data_content.zip")
            archive.write(path_file_data_model_zip, arcname="data_model.zip")

        # remove zipped content
        os.remove(path_file_data_content_zip)
        os.remove(path_file_data_model_zip)

    # prepare section
    def prepare_data_federation(self, path_file_data_federation_source):
        self.prepare_data_federation_for_path(path_file_data_federation_source, self.path_dir_dataset_store)

    def prepare_data_federation_for_path(
        self, path_file_data_federation_source: str, path_dir_dataset_prepared: str
    ) -> None:
        path_dir_data_federation_temp = tempfile.mkdtemp()
        # unzip entire data federation to tempdir
        with ZipFile(path_file_data_federation_source, "r") as archive:
            archive.extractall(path_dir_data_federation_temp)
        # remove the temp dir where everything was prepared for packaging
        path_dir_dataset_temp = os.path.join(path_dir_data_federation_temp, "dataset")
        for name_dataset_zip in os.listdir(path_dir_dataset_temp):

            path_file_dataset_source = os.path.join(path_dir_dataset_temp, name_dataset_zip)
            with ZipFile(path_file_dataset_source, "r") as archive:
                dataset_header = json.loads(archive.read("dataset_header.json"))
                dataset_id = dataset_header["dataset_id"]
                path_dir_dataset_target = os.path.join(path_dir_dataset_prepared, dataset_id)
                if os.path.isdir(path_dir_dataset_target):
                    shutil.rmtree(path_dir_dataset_target)
                archive.extractall(path_dir_dataset_target)
        shutil.rmtree(path_dir_data_federation_temp)

    def get_data_federation_packaged_header(self, path_file_data_federation_source):
        with ZipFile(path_file_data_federation_source, "r") as archive:
            return json.loads(archive.read("data_federation_header.json"))

    def get_dict_dataset_name_to_dataset_id(self, path_file_data_federation_source: str) -> Dict[str, str]:
        path_dir_data_federation_temp = tempfile.mkdtemp()
        with ZipFile(path_file_data_federation_source, "r") as archive:
            archive.extractall(path_dir_data_federation_temp)
        path_dir_dataset_temp = os.path.join(path_dir_data_federation_temp, "dataset")
        dict_dataset_name_to_dataset_id = {}
        for name_dataset_zip in os.listdir(path_dir_dataset_temp):
            path_file_dataset_source = os.path.join(path_dir_dataset_temp, name_dataset_zip)
            with ZipFile(path_file_dataset_source, "r") as archive:
                dataset_header = json.loads(archive.read("dataset_header.json"))
                dict_dataset_name_to_dataset_id[dataset_header["dataset_name"]] = dataset_header["dataset_id"]
        shutil.rmtree(path_dir_data_federation_temp)
        return dict_dataset_name_to_dataset_id

    # validation section

    def validate_data_federation_header(self, data_federation_header: Dict) -> bool:
        if "data_federation_id" not in data_federation_header:
            raise Exception("missing attribute: data_federation_id")
        return True

    def validate_dataset_header(self, dataset_header: Dict, data_federation_header: Dict) -> bool:
        if dataset_header["data_federation_id"] != data_federation_header["data_federation_id"]:
            data_federation_id_expected = data_federation_header["data_federation_id"]
            data_federation_id_found = dataset_header["data_federation_id"]
            raise Exception(
                f"inconsitent data_federation_id expected: {data_federation_id_expected} found {data_federation_id_found}"
            )
        if dataset_header["data_federation_name"] != data_federation_header["data_federation_name"]:
            data_federation_id_expected = data_federation_header["data_federation_name"]
            data_federation_id_found = dataset_header["data_federation_name"]
            raise Exception(
                f"inconsitent data_federation_name expected: {data_federation_id_expected} found {data_federation_id_found}"
            )
        # TODO check keys
        return True
