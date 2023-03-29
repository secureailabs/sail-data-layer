from enum import Enum


class DataTypeEnum(str, Enum):
    Categorical = "Categorical"
    Date = "Date"
    Datetime = "Datetime"
    Interval = "Interval"
    Unique = "Unique"

    def __str__(self) -> str:
        return str(self.value)

    @staticmethod
    def parse_str(value: str):
        if value == DataTypeEnum.Categorical:
            return DataTypeEnum.Categorical
        elif value == DataTypeEnum.Date:
            return DataTypeEnum.Date
        # TODO
        else:
            raise ValueError(f"Illegal enum value: {value}")
