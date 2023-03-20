from enum import Enum


class DatasetFormat(Enum):
    csvv1 = "csvv1"
    fhirv1 = "fhirv1"

    @staticmethod
    def parse_str(value: str):
        if value == "csvv1":
            return DatasetFormat.csvv1
        elif value == "fhirv1":
            return DatasetFormat.fhirv1
        else:
            raise ValueError(f"Illegal enum value: {value}")
