import os
from typing import Dict


class BaseDatasetSerializer:
    def __init__(self, dataset_packaging_format, path_dir_dataset_store) -> None:

        path_dir_dataset_store = path_dir_dataset_store
        self.path_dir_dataset_store = path_dir_dataset_store
        self.dataset_packaging_format = dataset_packaging_format
