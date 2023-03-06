from typing import Dict


class LongitudinalDatasetDataModel:
    def __init__(self) -> None:
        pass

    def to_dict(self):
        dict = {}
        dict["__type__"] = "LongitudinalDatasetDataModel"
        return dict

    @staticmethod
    def from_dict(dict: Dict):
        return LongitudinalDatasetDataModel()
