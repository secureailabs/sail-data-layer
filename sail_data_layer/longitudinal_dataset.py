from typing import Dict, List

from sail_data_layer.aggregator import Aggregator
from sail_data_layer.base_dataset import BaseDataset
from sail_data_layer.data_frame import DataFrame
from sail_data_layer.data_frame_data_model import DataFrameDataModel
from sail_data_layer.longitudinal_dataset_data_model import LongitudinalDatasetDataModel
from sail_data_layer.series import Series
from sail_data_layer.tabular_dataset import TabularDataset


class LongitudinalDataset(BaseDataset):
    def __init__(
        self,
        dataset_federation_id: str,
        dataset_federation_name: str,
        dataset_id: str,
        dataset_name: str,
        data_model: LongitudinalDatasetDataModel,
        list_patient: List,
    ) -> None:
        super().__init__(dataset_federation_id, dataset_federation_name, dataset_id, dataset_name)
        self.__data_model = data_model
        self.__list_patient = list_patient.copy()

    # property section start

    @property
    def data_model(self) -> LongitudinalDatasetDataModel:
        return self.__data_model

    # property section end

    def convert_to_data_frame(
        self,
        name_data_frame: str,
        list_aggregator: List[Aggregator],
    ) -> DataFrame:

        list_series = []
        for aggregator in list_aggregator:
            data_model_series = None  # TODO data_frame_data_model.get_data_model_series(series_name)
            list_data = []
            for patient in self.__list_patient:
                list_data.append(aggregator.agregate(patient))
            list_series.append(Series(self.dataset_id, data_model_series, list_data))
        # TODO return DataFrame(self.dataset_id, data_frame_data_model.data_frame_name, list_series)
        return DataFrame(self.dataset_id, name_data_frame, list_series)

    def compute_statistics(self) -> Dict:
        dict_measurement_statistics = {}
        for patient in self.__list_patient:
            for measurement in patient["dict_measurement"]:
                if measurement not in dict_measurement_statistics:
                    dict_measurement_statistics[measurement] = {}
                    dict_measurement_statistics[measurement]["count_atleastone"] = 0
                    dict_measurement_statistics[measurement]["count_total"] = 0
                    dict_measurement_statistics[measurement]["list_count"] = []
                    dict_measurement_statistics[measurement]["gini"] = 0  # TODO

                dict_measurement_statistics[measurement]["count_atleastone"] += 1
                dict_measurement_statistics[measurement]["count_total"] += len(patient["dict_measurement"][measurement])
                dict_measurement_statistics[measurement]["list_count"].append(
                    len(patient["dict_measurement"][measurement])
                )
        return dict_measurement_statistics

    def print_at_least_one(self) -> None:
        print(f"patient count : {len(self.__list_patient)}")
        dict_measurement_statistics = self.compute_statistics()

        for key, value in sorted(
            dict_measurement_statistics.items(),
            key=lambda key_value: key_value[1]["count_atleastone"],
            reverse=True,
        ):
            count = value["count_atleastone"] / len(self.__list_patient)
            print(f"{key} {count}")
