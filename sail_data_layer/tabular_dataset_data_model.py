from typing import Dict, List

from sail_data_layer.data_frame_data_model import DataFrameDataModel


class TabularDatasetDataModel:
    def __init__(self) -> None:
        self.__dict_data_frame_data_model = {}  # This could be an ordered dict but they do not map to json by default

    # index section start
    def __delitem__(self, key) -> None:
        raise NotImplementedError()

    def __getitem__(self, key) -> DataFrameDataModel:
        # TODO check key typing
        return self.get_data_frame_data_model(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    # index section end

    # property section start
    @property
    def list_data_frame_name(self) -> List[str]:
        return list(self.__dict_data_frame_data_model.keys())

    # property section end

    def get_data_frame_data_model(self, data_frame_name: str) -> DataFrameDataModel:
        if data_frame_name not in self.__dict_data_frame_data_model:
            raise Exception(f"No such data_frame_model: {data_frame_name}")
        return self.__dict_data_frame_data_model[data_frame_name]

    def add_data_frame_data_model(self, data_frame_data_model: DataFrameDataModel) -> None:
        if data_frame_data_model.data_frame_name in self.__dict_data_frame_data_model:
            raise Exception(f"Duplicate data_frame_model: {data_frame_data_model.data_frame_name}")
        self.__dict_data_frame_data_model[data_frame_data_model.data_frame_name] = data_frame_data_model

    def to_dict(self) -> Dict:
        dict = {}
        dict["__type__"] = "TabularDatasetDataModel"
        dict["dict_data_frame_data_model"] = {}
        for table_id, data_frame_data_model in self.__dict_data_frame_data_model.items():
            dict["dict_data_frame_data_model"][table_id] = data_frame_data_model.to_dict()
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "TabularDatasetDataModel":
        data_model_tabular = TabularDatasetDataModel()
        for table_id, data_frame_data_model in dict["dict_data_frame_data_model"].items():
            data_model_tabular.__dict_data_frame_data_model[table_id] = DataFrameDataModel.from_dict(
                data_frame_data_model
            )

        return data_model_tabular
