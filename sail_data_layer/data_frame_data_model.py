import uuid
from typing import Dict, List, Optional, Tuple

from sail_data_layer.series_data_model import SeriesDataModel


class DataFrameDataModel:
    def __init__(
        self,
        data_frame_name: str,
        data_frame_data_model_id: Optional[str] = None,
        list_series_data_model: List[SeriesDataModel] = [],
    ) -> None:
        self.__data_frame_name = data_frame_name
        if data_frame_data_model_id is None:
            self.__data_frame_data_model_id = str(uuid.uuid4())
        else:
            self.__data_frame_data_model_id = data_frame_data_model_id
        self.__list_series_data_model: List[SeriesDataModel] = []
        self.__dict_series_data_model = {}  # TODO This could be an ordered dict
        for series_data_model in list_series_data_model:
            self._add_series_data_model(series_data_model)

    # index section start
    def __delitem__(self, key) -> None:
        raise NotImplementedError()

    def __getitem__(self, key) -> SeriesDataModel:
        # TODO check key typing
        return self.get_series_data_model(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    # index section end

    # property section start
    @property
    def data_frame_name(self) -> str:
        return self.__data_frame_name

    @property
    def data_frame_data_model_id(self) -> str:
        return self.__data_frame_data_model_id

    @property
    def list_series_name(self) -> List[str]:
        return list(self.__dict_series_data_model.keys())

    @property
    def list_series_data_model(self) -> List[SeriesDataModel]:
        return self.__list_series_data_model.copy()

    # property section end

    # TODO type dataframe without avoiding cyclic dependance USE interface!
    def validate(self, data_frame, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        # TODO validate that patient id and dataset id series are present?

        # validate all data sereis present
        list_series_name_model = self.list_series_name
        list_series_name_data_frame = data_frame.list_series_name
        for name in list_series_name_model:
            if name not in list_series_name_data_frame:
                list_problem.append(
                    f"In data frame with name {self.data_frame_name} series with name {name} specified in model but not present in data frame"
                )
        for name in list_series_name_data_frame:
            if name not in list_series_name_model:
                list_problem.append(
                    f"In data frame with name {self.data_frame_name} series with name {name} present in data frame but not specified in model"
                )

        for name in list_series_name_data_frame:
            if name in list_series_name_model:
                self[name].validate(self.data_frame_name, data_frame[name], list_problem)

        return len(list_problem) == 0, list_problem

    def get_series_data_model(self, series_name: str) -> SeriesDataModel:
        if series_name not in self.__dict_series_data_model:
            raise Exception(f"No such series: {series_name}")
        return self.__dict_series_data_model[series_name]

    def _add_series_data_model(self, series_data_model: SeriesDataModel) -> None:
        if series_data_model.series_name in self.__dict_series_data_model:
            raise Exception(f"Duplicate series: {series_data_model.series_name}")
        self.__list_series_data_model.append(series_data_model)
        self.__dict_series_data_model[series_data_model.series_name] = series_data_model

    def to_dict(self) -> Dict:
        dict_json = {}
        dict_json["__type__"] = "DataFrameDataModel"
        dict_json["data_frame_name"] = self.data_frame_name
        dict_json["data_frame_data_model_id"] = self.data_frame_data_model_id
        dict_json["list_series_data_model"] = []  # TODO we want to have a list here not a dict
        for series_data_model in self.__list_series_data_model:
            dict_json["list_series_data_model"].append(series_data_model.to_dict())
        return dict_json

    @staticmethod
    def from_dict(dict_json: Dict) -> "DataFrameDataModel":
        list_series_data_model = []
        for dict_series_json in dict_json["list_series_data_model"]:
            list_series_data_model.append(SeriesDataModel.from_dict(dict_series_json))
        return DataFrameDataModel(
            dict_json["data_frame_name"], dict_json["data_frame_data_model_id"], list_series_data_model
        )
