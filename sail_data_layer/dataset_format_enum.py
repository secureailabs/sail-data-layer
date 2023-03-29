from enum import Enum


class DatasetFormatEnum(str, Enum):
    csvv1 = "csvv1"
    fhirv1 = "fhirv1"

    def __str__(self) -> str:
        return str(self.value)

    @staticmethod
    def parse_str(value: str):
        if value == "csvv1":
            return DatasetFormatEnum.csvv1
        elif value == "fhirv1":
            return DatasetFormatEnum.fhirv1
        else:
            raise ValueError(f"Illegal enum value: {value}")
