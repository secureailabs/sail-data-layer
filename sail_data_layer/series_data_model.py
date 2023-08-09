import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sail_data_layer.data_type_enum import DataTypeEnum


class SeriesDataModel(ABC):
    def __init__(
        self,
        name: str,
        data_type: DataTypeEnum,
        series_id: Optional[str] = None,
    ) -> None:

        # if type_data_level == SeriesDataModel.DataLevelCategorical:
        #     if list_value is None:
        #         raise ValueError(f"list_value cannot be None")
        #     if len(list_value) == 0:
        #         raise ValueError(f"list_value must be at least size 1")
        #     if len(list_value) != len(set(list_value)):
        #         raise ValueError(f"list_value can only contain unique values")

        self.__name = name
        if id is None:
            self.series_id = str(uuid.uuid4())
        else:
            self.series_id = series_id
        self.__data_type = data_type

    # properties
    @property
    def name(self) -> str:
        return self.__name

    @property
    def data_type(self) -> DataTypeEnum:
        return self.__data_type

    @property
    def id(self) -> str:
        return self.id

    # methods
    def _get_problem_prefix(self, name_data_frame):
        return f"In data frame with name {name_data_frame} in series with name {self.name} "

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModel":
        if dict["series_schema"]["type"] == "SeriesDataModelCategorical":
            return SeriesDataModelCategorical.from_dict(dict)
        elif dict["series_schema"]["type"] == "SeriesDataModelDate":
            return SeriesDataModelDate.from_dict(dict)
        elif dict["series_schema"]["type"] == "SeriesDataModelDateTime":
            return SeriesDataModelDateTime.from_dict(dict)
        elif dict["series_schema"]["type"] == "SeriesDataModelInterval":
            return SeriesDataModelInterval.from_dict(dict)
        elif dict["series_schema"]["type"] == "SeriesDataModelUnique":
            return SeriesDataModelUnique.from_dict(dict)

        else:
            raise Exception(f"Unkown type {0}", dict["series_schema"]["type"])

    @abstractmethod
    def to_dict(self) -> Dict:
        raise NotImplementedError()


class SeriesDataModelCategorical(SeriesDataModel):
    def __init__(
        self,
        name: str,
        list_value: List[str],
        id: Optional[str] = None,
    ) -> None:
        super().__init__(name, DataTypeEnum.Categorical, id)
        if len(list_value) == 0:
            raise ValueError(f"Lenght of 'list_value' must be at least size 1")
        self.__list_value = list_value

    # property
    @property
    def list_value(self) -> List[str]:
        return self.__list_value.copy()

    # TODO type dataframe without avoiding cyclic dependance USE interface!
    def validate(self, name_data_frame: str, series, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        problem_prefix = self._get_problem_prefix(name_data_frame)

        # check that the series.dtype is string
        # check that every value is out of the list of values or None
        for index, value in series.items():  # TODO index should be patient_id
            if not isinstance(value, str):
                if value is not None:  # TODO currently every value is allowed to be None
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value is not of type string but of type {str(type(value))} while date model specifies this is series as Categorical"
                    )
            if str(value) not in self.list_value:
                if value is not None:  # TODO currently every value is allowed to be None
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value is {str(value)} which is not in the list of allowed values"
                    )
        return len(list_problem) == 0, list_problem

    def to_dict(self) -> Dict:
        dict = {}
        dict["series_schema"]["type"] = "SeriesDataModelCategorical"
        dict["name"] = self.name
        dict["list_value"] = self.list_value
        dict["id"] = self.id
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModelCategorical":
        if dict["series_schema"]["type"] != "SeriesDataModelCategorical":
            raise Exception(f"invalid type {dict['__type__']}")

        return SeriesDataModelCategorical(
            dict["name"],
            dict["series_schema"]["list_value"],
            dict["id"],
        )


class SeriesDataModelDate(SeriesDataModel):
    def __init__(
        self,
        name: str,
        id: Optional[str] = None,
    ) -> None:
        super().__init__(name, DataTypeEnum.Date, id)

    # TODO type dataframe without avoiding cyclic dependance USE interface!
    def validate(self, name_data_frame: str, series, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        problem_prefix = self._get_problem_prefix(name_data_frame)

        # check that the series.dtype is timestamp
        # check that every value is out of the list of values or None
        # check that every value is a date and not a datetime
        # check that not timezone is specified
        for index, value in series.items():  # TODO index should be patient_id
            if value is not None:  # TODO currently every value is allowed to be None
                if not isinstance(value, datetime):  # TODO check if this is correct
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value is not of type datetime but of type {str(type(value))} while date model specifies this is series as Date"
                    )

                if value.hour != 0 or value.minute != 0 or value.second != 0 or value.microsecond != 0:
                    list_problem.append(
                        problem_prefix + f" at index {index} value is not a date but a datetime with value {str(value)}"
                    )
                if value.tzinfo is not None:
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value has a timezone specified with value {str(value.tzinfo)}"
                    )
        return len(list_problem) == 0, list_problem

    def to_dict(self) -> Dict:
        dict = {}
        dict["series_schema"]["type"] = "SeriesDataModelDate"
        dict["name"] = self.name
        dict["id"] = self.id
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModelDate":
        if dict["series_schema"]["type"] != "SeriesDataModelDate":
            raise Exception(f"invalid type {dict['__type__']}")

        return SeriesDataModelDate(
            dict["name"],
            dict["id"],
        )


class SeriesDataModelDateTime(SeriesDataModel):
    def __init__(
        self,
        name: str,
        id: Optional[str] = None,
    ) -> None:
        super().__init__(name, DataTypeEnum.Datetime, id)

    # TODO type dataframe without avoiding cyclic dependance USE interface!
    def validate(self, name_data_frame: str, series, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        problem_prefix = self._get_problem_prefix(name_data_frame)

        # check that the series.dtype is timestamp
        # check that every value is out of the list of values or None
        # check that a timezone is specified
        for index, value in series.items():  # TODO index should be patient_id
            if value is not None:  # TODO currently every value is allowed to be None
                if not isinstance(value, datetime):  # TODO check if this is correct
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value is not of type datatime but of type {str(type(value))} while date model specifies this is series as DateTime"
                    )

                if value.tzinfo is None:
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value has no timezone specified while date model specifies this is series as DateTime."
                    )
        return len(list_problem) == 0, list_problem

    def to_dict(self) -> Dict:
        dict = {}
        dict["series_schema"]["type"] = "SeriesDataModelDateTime"
        dict["name"] = self.name
        dict["id"] = self.id
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModelDateTime":
        if dict["series_schema"]["type"] != "SeriesDataModelDateTime":
            raise Exception(f"invalid type {dict['__type__']}")

        return SeriesDataModelDateTime(
            dict["name"],
            dict["id"],
        )


class SeriesDataModelInterval(SeriesDataModel):
    def __init__(
        self,
        name: str,
        id: Optional[str] = None,
        *,
        unit: str = "unitless",
        min: Optional[float] = None,
        max: Optional[float] = None,
        resolution: Optional[float] = None,
    ) -> None:
        super().__init__(name, DataTypeEnum.Interval, id)
        self.__unit = unit
        self.__min = min
        self.__max = max
        self.__resolution = resolution

    # property
    @property
    def unit(self) -> str:
        return self.__unit

    @property
    def min(self) -> Optional[float]:
        return self.__min

    @property
    def max(self) -> Optional[float]:
        return self.__max

    @property
    def resolution(self) -> Optional[float]:
        return self.__resolution

    # method

    def validate(self, name_data_frame, series, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        problem_prefix = self._get_problem_prefix(name_data_frame)
        # check that the series.dtype is float
        # check that every value is between min an max if specified
        # check that every value is in resolution if specified
        for index, value in series.items():  # TODO index should be patient_id
            if not isinstance(value, float):
                list_problem.append(
                    problem_prefix
                    + f"at index {index} value is not of type float but of type {str(type(value))} while date model specifies this is series as Interval"
                )
                continue
            if self.min is not None:
                if value < self.min:
                    list_problem.append(
                        problem_prefix
                        + f"at index {index} value is smaller than specified minimum of : {str(self.min)}"
                    )

            if self.max is not None:
                if self.max < value:
                    list_problem.append(
                        problem_prefix + f"at index {index} value is larger than specified maximum of : {str(self.max)}"
                    )

            if self.resolution is not None:
                if 0.0001 < (value % self.resolution):
                    list_problem.append(
                        problem_prefix
                        + f"at index {index} value is out of specified resultion  {str(self.resolution)} by more than 0.0001"
                    )
        return len(list_problem) == 0, list_problem

    def to_dict(self) -> Dict:
        dict = {}
        dict["series_schema"]["type"] = "SeriesDataModelInterval"
        dict["name"] = self.name
        dict["id"] = self.id
        dict["unit"] = self.unit
        dict["min"] = self.min
        dict["max"] = self.max
        dict["resolution"] = self.resolution
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModelInterval":
        if dict["series_schema"]["type"] != "SeriesDataModelInterval":
            raise Exception(f"invalid type {dict['__type__']}")

        return SeriesDataModelInterval(
            dict["name"],
            dict["id"],  # TODO pass other parameters
            unit=dict["series_schema"]["unit"],
            min=dict["series_schema"]["min"],
            max=dict["series_schema"]["max"],
            resolution=dict["series_schema"]["resolution"],
        )


class SeriesDataModelUnique(SeriesDataModel):
    def __init__(
        self,
        name: str,
        id: Optional[str] = None,
    ) -> None:
        super().__init__(name, DataTypeEnum.Unique, id)

    def validate(self, name_data_frame, series, list_problem: List[str] = []) -> Tuple[bool, List[str]]:
        problem_prefix = self._get_problem_prefix(name_data_frame)
        # check that the series type is string
        for index, value in series.items():  # TODO index should be patient_id
            if value is not None:  # TODO currently every value is allowed to be None
                if not isinstance(value, str):
                    list_problem.append(
                        problem_prefix
                        + f" at index {index} value is not of type string but of type {str(type(value))} while date model specifies this is series as Unique"
                    )
        return len(list_problem) == 0, list_problem

    def to_dict(self) -> Dict:
        dict = {}
        dict["series_schema"]["type"] = "SeriesDataModelUnique"
        dict["name"] = self.name
        dict["id"] = self.id
        return dict

    @staticmethod
    def from_dict(dict: Dict) -> "SeriesDataModelUnique":
        if dict["series_schema"]["type"] != "SeriesDataModelUnique":
            raise Exception(f"invalid type {0}", dict["series_schema"]["type"])

        return SeriesDataModelUnique(
            dict["name"],
            dict["id"],
        )
