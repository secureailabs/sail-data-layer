import json
import random
import uuid
from datetime import datetime, timedelta, tzinfo
from typing import List

import numpy as np
import pytz
from pandas import DataFrame, Series


class GeneratorBase:
    def __init__(self, name_series: str) -> None:
        self.name_series = name_series
        self.list_interaction = []

    def generate_series(self, data_frame: DataFrame) -> Series:

        if len(data_frame.columns) == 0:
            list_instance = data_frame.index
        else:
            list_instance = data_frame.to_dict(
                orient="records",
            )
        list_value = []
        for instance in list_instance:
            list_value.append(self.generate_instance(instance))
        return Series(list_value, data_frame.index, name=self.name_series)

    def generate_instance(self, instance: dict):
        raise NotImplementedError()


class GeneratorUniqueUuid(GeneratorBase):
    def __init__(
        self,
        name_series: str,
    ) -> None:
        super().__init__(name_series)

    def generate_instance(self, instance: dict) -> str:
        return str(uuid.uuid4())


class GeneratorDateNormal(GeneratorBase):
    def __init__(self, name_series: str, datetime_mean: datetime, standard_deviation_days: float) -> None:
        super().__init__(name_series)
        self.datetime_mean = datetime_mean
        self.standard_deviation_days = standard_deviation_days

    def generate_instance(self, instance: dict) -> str:
        random_days = timedelta(random.normalvariate(0, self.standard_deviation_days))
        return (self.datetime_mean + random_days).strftime("%Y-%m-%d")


class GeneratorDatetimeNormal(GeneratorBase):
    def __init__(
        self, name_series: str, datetime_mean: datetime, timezone: tzinfo, standard_deviation_days: float
    ) -> None:
        super().__init__(name_series)
        print(name_series)
        self.datetime_mean = datetime_mean
        self.timezone = timezone
        self.standard_deviation_days = standard_deviation_days

    def generate_instance(self, instance: dict) -> str:
        random_days = timedelta(random.normalvariate(0, self.standard_deviation_days))
        datetime_random = self.datetime_mean + random_days
        datetime_random = datetime_random.replace(tzinfo=self.timezone)
        return datetime_random.strftime("%Y-%m-%d %H:%M:%S %z")


class GeneratorIntervalNormal(GeneratorBase):
    def __init__(self, name_series: str, value_mean: float, value_standard_deviation: float) -> None:
        super().__init__(name_series)
        self.value_mean = value_mean
        self.value_standard_deviation = value_standard_deviation

    def generate_instance(self, instance: dict) -> float:
        value_random = random.normalvariate(self.value_mean, self.value_standard_deviation)
        return random.normalvariate(self.value_mean, self.value_standard_deviation)


class GeneratorDateOffserExponention(GeneratorBase):
    def __init__(self, name_series: str, name_series_offset: str, offset_mean_days: float) -> None:
        super().__init__(name_series)
        self.name_series_offset = name_series_offset
        self.offset_mean_days = offset_mean_days
        self.list_interaction = []

    def generate_instance(self, instance: dict) -> str:
        if self.name_series_offset not in instance:
            raise ValueError()
        date_base = datetime.strptime(instance[self.name_series_offset], "%Y-%m-%d")

        offset_mean_days = self.offset_mean_days
        for interaction in self.list_interaction:
            if instance[interaction["name_series_cause"]] == interaction["value_cause"]:
                offset_mean_days *= interaction["hazzard_ratio"]
        random_days = timedelta(np.random.exponential(scale=offset_mean_days))
        return (date_base + random_days).strftime("%Y-%m-%d")


class GeneratorCategory(GeneratorBase):
    def __init__(self, name_series: str, list_value_generator: List) -> None:
        super().__init__(name_series)
        self.list_value = []
        self.list_weight = []
        for value_generator in list_value_generator:
            self.list_value.append(value_generator["value"])
            self.list_weight.append(value_generator["weight_base"])

    def generate_instance(self, instance: dict):
        array_limit = np.array(self.list_weight)

        for interaction in self.list_interaction:
            if instance[interaction["name_series_cause"]] == interaction["value_cause"]:
                for i, value in enumerate(self.list_value):
                    if value == interaction["value_affects"]:
                        array_limit[i] *= interaction["hazzard_ratio"]
        array_limit = array_limit / np.sum(array_limit)
        array_limit = np.cumsum(array_limit)

        rng = random.random()
        index_value = 0
        while array_limit[index_value] < rng:
            index_value += 1
        return self.list_value[index_value]
