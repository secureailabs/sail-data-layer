from typing import Dict, List, Tuple

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

    # TODO type dataset without avoiding cyclic dependance USE interface!
    def validate(self, dataset) -> Tuple[bool, List[str]]:
        list_problem = []

        # validate all data frames present
        list_data_frame_name_model = self.list_data_frame_name
        list_data_frame_name_dataset = dataset.list_data_frame_name
        for name in list_data_frame_name_model:
            if name not in list_data_frame_name_dataset:
                list_problem.append(f"Dataframe with name {name} specified in model but not present in dataset")
        for name in list_data_frame_name_dataset:
            if name not in list_data_frame_name_model:
                list_problem.append(f"Dataframe with name {name} present in dataset but not specified in model")

        for name in list_data_frame_name_dataset:
            if name in list_data_frame_name_model:
                self[name].validate(dataset[name], list_problem)

        return len(list_problem) == 0, list_problem

    def get_data_frame_data_model(self, data_frame_name: str) -> DataFrameDataModel:
        if data_frame_name not in self.__dict_data_frame_data_model:
            raise Exception(f"No such data_frame_model: {data_frame_name}")
        return self.__dict_data_frame_data_model[data_frame_name]

    def add_data_frame_data_model(self, data_frame_data_model: DataFrameDataModel) -> None:
        if data_frame_data_model.data_frame_name in self.__dict_data_frame_data_model:
            raise Exception(f"Duplicate data_frame_model: {data_frame_data_model.data_frame_name}")
        self.__dict_data_frame_data_model[data_frame_data_model.data_frame_name] = data_frame_data_model

    def to_dict(self) -> Dict:
        # TODO TECHdebt dict_data_frame_data_model makes more sense dict_data_model_data_frame is a legacy string but
        # all the datasets where generated with the old code so lets keep the seriealizer as is
        dict = {}
        dict["__type__"] = "TabularDatasetDataModel"
        dict["dict_data_model_data_frame"] = {}
        for table_id, data_frame_data_model in self.__dict_data_frame_data_model.items():
            dict["dict_data_model_data_frame"][table_id] = data_frame_data_model.to_dict()
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "TabularDatasetDataModel":
        data_model_tabular = TabularDatasetDataModel()
        for table_id, data_frame_data_model in dict["dict_data_model_data_frame"].items():
            data_model_tabular.__dict_data_frame_data_model[table_id] = DataFrameDataModel.from_dict(
                data_frame_data_model
            )

        return data_model_tabular
