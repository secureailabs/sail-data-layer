from io import BytesIO, StringIO
from typing import List

import pandas
from pandas import DataFrame as DataFramePandas

from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.series import Series


class DataFrame(DataFramePandas):
    # NOTE Long term this overloading trick is not maintainable and we will need to create a
    # class where the pandas object in a member not a superclass

    def __init__(self, dataset_id: str, data_frame_name: str, list_series: List[Series]) -> None:
        super().__init__()
        self.dataset_id = dataset_id
        self.data_frame_name = data_frame_name
        self.data_frame_data_model = DataFrameDataModel(data_frame_name)
        for series in list_series:
            self._add_series(series)

    def _add_series(self, series: Series):
        if series.series_name in self.columns:
            raise ValueError(f"Duplicate series: {series.series_name}")
        # TODO overload this indexer as well !!!!
        super().__setitem__(series.series_name, series)
        self.data_frame_data_model.add_data_model_series(series.data_model_series)

    def get_series(self, series_name: str) -> Series:
        if series_name not in self.list_series_name:
            raise Exception(f"No such series: {series_name}")
        return Series(
            self.dataset_id,
            self.data_frame_data_model[series_name],
            super().__getitem__(series_name).to_list(),
        )

    def select_series(self, list_series_name: List[str]) -> "DataFrame":
        list_series = []
        for series_name in list_series_name:
            if series_name not in self.list_series_name:
                raise Exception(f"No such series: {series_name}")
            list_series.append(self.get_series(series_name))
        return DataFrame(self.dataset_id, self.data_frame_name, list_series)

    # index section start
    def __delitem__(self, key) -> None:
        raise NotImplementedError()

    def __getitem__(self, key) -> Series:
        # TODO check key typing
        return self.get_series(key)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    # index section end

    # property section start
    @property
    def list_series_name(self) -> List[str]:
        return list(self.columns)

    # property section end

    @staticmethod
    def from_csv(
        dataset_id: str, data_frame_name: str, data_frame_data_model: DataFrameDataModel, path_file_csv: str
    ) -> "DataFrame":
        with open(path_file_csv, "rb") as file:
            return DataFrame.from_csv_str(dataset_id, data_frame_name, data_frame_data_model, file.read())

    @staticmethod
    def from_csv_str(
        dataset_id: str,
        data_frame_name: str,
        data_frame_data_model: DataFrameDataModel,
        csv_content: str,
    ) -> "DataFrame":
        data_frame_pandas = pandas.read_csv(BytesIO(csv_content))
        return DataFrame.from_pandas(dataset_id, data_frame_name, data_frame_data_model, data_frame_pandas)

    @staticmethod
    def from_pandas(
        dataset_id: str,
        data_frame_name: str,
        data_frame_data_model: DataFrameDataModel,
        data_frame_pandas: DataFramePandas,
    ) -> "DataFrame":
        list_series_name = data_frame_data_model.list_series_name
        list_series = []
        for series_name in data_frame_data_model.list_series_name:
            series = Series.from_pandas(dataset_id, data_frame_data_model[series_name], data_frame_pandas[series_name])
            list_series.append(series)
            list_series_name.remove(series_name)

        if 0 < len(list_series_name):
            raise Exception(f"Missing series: {list(list_series_name)}")
        return DataFrame(dataset_id, data_frame_name, list_series)

    # TODO check what feature we use on the Pandas data_frame that return pandas data_frame or series, those will need overloading
