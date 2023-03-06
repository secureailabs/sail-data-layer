from typing import Dict, List

from sail_data_layer.base_dataset import BaseDataset
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.tabular_dataset_data_model import TabularDatasetDataModel


class TabularDataset(BaseDataset):
    def __init__(
        self,
        dataset_federation_id: str,
        dataset_federation_name: str,
        dataset_id: str,
        dataset_name: str,
        list_data_frame: List[DataFrame],
    ) -> None:
        super().__init__(dataset_federation_id, dataset_federation_name, dataset_id, dataset_name)
        # TODO have the data_model validate the dict_table

        self.__data_model = TabularDatasetDataModel()
        self.__dict_data_frame = {}
        for data_frame in list_data_frame:
            self.add_data_frame(data_frame)

    # index section start
    def __delitem__(self, key) -> None:
        raise NotImplementedError()

    def __getitem__(self, key) -> DataFrame:
        # TODO check key typing
        return self.get_data_frame(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    # index section end

    # property section start
    @property
    def list_data_frame_name(self) -> List[str]:
        return list(self.__data_model.list_data_frame_name)

    @property
    def data_model(self) -> TabularDatasetDataModel:
        return self.__data_model

    # property section end

    def get_data_frame(self, data_frame_name: str) -> DataFrame:
        if data_frame_name not in self.__dict_data_frame:
            raise Exception(f"No such data_frame_model: {data_frame_name}")
        return self.__dict_data_frame[data_frame_name]

    def add_data_frame(self, data_frame: DataFrame):
        if data_frame is None:
            raise ValueError(f"data_frame cannot be `None`")
        if data_frame.data_frame_name in self.__dict_data_frame:
            raise ValueError(f"Duplicate data_frame: {data_frame.data_frame_name}")
        self.__dict_data_frame[data_frame.data_frame_name] = data_frame
        self.__data_model.add_data_frame_data_model(data_frame.data_frame_data_model)
