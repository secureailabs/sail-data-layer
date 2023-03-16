from typing import Dict, List, Tuple

from sail_data_layer.series_data_model import SeriesDataModel


class DataFrameDataModel:
    def __init__(self, data_frame_name) -> None:
        self.data_frame_name = data_frame_name
        self.dict_data_model_series = {}  # This could be an ordered dict but they do not map to json by default

    # index section start
    def __delitem__(self, key) -> None:
        raise NotImplementedError()

    def __getitem__(self, key) -> SeriesDataModel:
        # TODO check key typing
        return self.get_data_model_series(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    # index section end

    # property section start
    @property
    def list_series_name(self) -> List[str]:
        return list(self.dict_data_model_series.keys())

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

    def get_data_model_series(self, series_name: str) -> SeriesDataModel:
        if series_name not in self.dict_data_model_series:
            raise Exception(f"No such series: {series_name}")
        return self.dict_data_model_series[series_name]

    def add_data_model_series(self, data_model_series: SeriesDataModel) -> None:
        if data_model_series.series_name in self.dict_data_model_series:
            raise Exception(f"Duplicate series: {data_model_series.series_name}")
        self.dict_data_model_series[data_model_series.series_name] = data_model_series

    def to_dict(self) -> Dict:
        dict = {}
        dict["__type__"] = "DataFrameDataModel"
        dict["data_frame_name"] = self.data_frame_name
        dict["dict_data_model_series"] = {}
        for name_feature in self.dict_data_model_series:
            dict["dict_data_model_series"][name_feature] = self.get_data_model_series(name_feature).to_dict()
        return dict

    @staticmethod
    def from_dict(dict_json: Dict) -> "DataFrameDataModel":
        data_model_tabular = DataFrameDataModel(dict_json["data_frame_name"])
        for name_feature in dict_json["dict_data_model_series"]:
            data_model_tabular.dict_data_model_series[name_feature] = SeriesDataModel.from_dict(
                dict_json["dict_data_model_series"][name_feature]
            )
        return data_model_tabular
