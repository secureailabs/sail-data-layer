import os
from typing import Dict


class BaseDatasetSerializer:
    def __init__(self, dataset_packaging_format) -> None:

        path_dir_dataset_store = os.environ.get("PATH_DIR_DATASET")
        if path_dir_dataset_store is None:
            raise Exception("Evironment variable: `PATH_DIR_DATASET` not set")

        self.path_dir_dataset_store = path_dir_dataset_store
        self.dataset_packaging_format = dataset_packaging_format
