import json
import os
import shutil
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pandas

from sail_data_layer.base_dataset_serializer import BaseDatasetSerializer
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.tabular_dataset import TabularDataset
from sail_data_layer.tabular_dataset_data_model import TabularDatasetDataModel


class Csvv1DatasetSerializer(BaseDatasetSerializer):
    # A general note on the ZipFile package: The initializer of ZipFile takes a fourth argument called allowZip64.
    # It’s a Boolean argument that tells ZipFile to create ZIP files with the .zip64 extension for files larger than 4 GB.

    # zipfile.ZIP_DEFLATED	Deflate	zlib
    # zipfile.ZIP_BZIP2	    Bzip2	bz2
    # zipfile.ZIP_LZMA	    LZMA	lzma
    def __init__(self) -> None:
        super().__init__("csvv1")

    def read_dataset(self, dataset_id) -> TabularDataset:
        return self.read_dataset_for_path(os.path.join(self.path_dir_dataset_store, dataset_id))

    def read_dataset_for_path(self, path_dir_dataset_source) -> TabularDataset:
        # TODO check signature

        path_file_dataset_header = os.path.join(path_dir_dataset_source, "dataset_header.json")
        path_file_data_model = os.path.join(path_dir_dataset_source, "data_model.zip")
        path_file_data_content = os.path.join(path_dir_dataset_source, "data_content.zip")
        with open(path_file_dataset_header, "r") as file:
            header_dataset = json.load(file)
        data_federation_id = header_dataset["data_federation_id"]
        data_federation_name = header_dataset["data_federation_name"]
        dataset_id = header_dataset["dataset_id"]
        dataset_name = header_dataset["dataset_name"]
        # TODO check header
        with ZipFile(path_file_data_model) as archive_data_model:
            data_model_tabular = TabularDatasetDataModel.from_dict(json.loads(archive_data_model.read("data_model.json")))
        list_data_frame = []
        with ZipFile(path_file_data_content) as archive_data_content:
            for name_file in archive_data_content.namelist():
                if not name_file.endswith(".csv"):
                    raise Exception()
                data_frame_name = name_file.split(".csv")[0]
                data_model_data_frame = data_model_tabular[data_frame_name]
                list_data_frame.append(
                    DataFrame.from_csv_str(
                        dataset_id,
                        data_frame_name,
                        data_model_data_frame,
                        archive_data_content.read(name_file),
                    )
                )
        return TabularDataset(data_federation_id, data_federation_name, dataset_id, dataset_name, list_data_frame)

    def write_dataset(self, tabular_dataset: TabularDataset) -> None:
        self.write_dataset_for_path(
            os.path.join(self.path_dir_dataset_store, tabular_dataset.dataset_id), tabular_dataset
        )

    def write_dataset_for_path(self, path_dir_dataset_target: str, tabular_dataset: TabularDataset) -> None:
        if os.path.isdir(path_dir_dataset_target):
            shutil.rmtree(path_dir_dataset_target)
        os.makedirs(path_dir_dataset_target)
        path_file_dataset_header = os.path.join(path_dir_dataset_target, "dataset_header.json")
        path_file_data_model = os.path.join(path_dir_dataset_target, "data_model.zip")
        path_file_data_content = os.path.join(path_dir_dataset_target, "data_content.zip")

        # write dataset header
        header_dataset = {}
        header_dataset["data_federation_id"] = tabular_dataset.dataset_federation_id
        header_dataset["data_federation_name"] = tabular_dataset.dataset_federation_name
        header_dataset["dataset_id"] = tabular_dataset.dataset_id
        header_dataset["dataset_name"] = tabular_dataset.dataset_name

        with open(path_file_dataset_header, "w") as file:
            json.dump(header_dataset, file)

        # write data model
        with ZipFile(path_file_data_model, "w", ZIP_DEFLATED, compresslevel=9) as zip_file_data_model:
            name_file = "data_model.json"
            zip_file_data_model.writestr(name_file, json.dumps(tabular_dataset.data_model.to_dict()))

        # write data content
        with ZipFile(path_file_data_content, "w", ZIP_DEFLATED, compresslevel=9) as zip_file_data_content:
            for data_frame_name in tabular_dataset.list_data_frame_name:
                data_frame = tabular_dataset[data_frame_name]
                # saving a data frame to a buffer (same as with a regular file):
                buffer = BytesIO()
                data_frame.to_csv(buffer)
                buffer.seek(0)
                # write buffer to zip
                name_file = data_frame_name + ".csv"
                zip_file_data_content.writestr(name_file, buffer.read())
