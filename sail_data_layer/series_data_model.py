import json
import statistics
from typing import Dict, List, Optional


class SeriesDataModel:
    DataLevelUnique = "DataLevelUnique"
    DataLevelCategorical = "DataLevelCategorical"
    DataLevelInterval = "DataLevelInterval"

    MissingPolicyPropagateAddColumn = "MissingPolicyPropagateAddColumn"
    MissingPolicyRaiseException = "MissingPolicyRaiseException"

    AgregatorCsv = "AgregatorCsv"
    AgregatorComputed = "AgregatorComputed"
    AgregatorPatientGender = "AgregatorPatientGender"
    AgregatorPatientMaritalStatus = "AgregatorPatientMaritalStatus"
    AgregatorPatientRace = "AgregatorPatientRace"
    AgregatorPatientEthnicity = "AgregatorPatientEthnicity"
    AgregatorIntervalFirstOccurance = "AgregatorIntervalFirstOccurance"
    AgregatorIntervalLastOccurance = "AgregatorIntervalLastOccurance"
    AgregatorIntervalCountOccurance = "AgregatorIntervalCountOccurance"
    AgregatorIntervalMean = "AgregatorIntervalMean"
    AgregatorCategoricalFirstOccurance = "AgregatorCategoricalFirstOccurance"
    AgregatorCategoricalLastOccurance = "AgregatorCategoricalLastOccurance"
    AgregatorCategoricalCountOccurance = "AgregatorCategoricalCountOccurance"
    AgregatorCategoricalMostFrequent = "AgregatorCategoricalMostFrequent"

    def __init__(
        self,
        series_name: str,
        type_data_level: str,
        *,
        unit: str = None,
        value_min: str = None,
        value_max: str = None,
        resolution: float = None,
        list_value: List[str] = None,
        measurement_source_name: str = None,
        type_agregator: str = None,
    ) -> None:
        if type_data_level not in [
            SeriesDataModel.DataLevelUnique,
            SeriesDataModel.DataLevelCategorical,
            SeriesDataModel.DataLevelInterval,
        ]:
            raise ValueError(f"Illegal value data_level: {type_data_level}")

        if type_data_level == SeriesDataModel.DataLevelCategorical:
            if list_value is None:
                raise ValueError(f"list_value cannot be None")
            if len(list_value) == 0:
                raise ValueError(f"list_value must be at least size 1")
            if len(list_value) != len(set(list_value)):
                raise ValueError(f"list_value can only contain unique values")

        self.series_name = series_name
        self.type_data_level = type_data_level

        self.unit = unit
        self.value_min = value_min
        self.value_max = value_max
        self.resolution = resolution  # for numerical

        self.list_value = list_value  # for cathegorical

        self.type_agregator = type_agregator
        self.measurement_source_name = measurement_source_name

    def agregate(self, patient):
        try:
            # patient resource lookup
            # TODO refactor this
            if self.type_data_level == SeriesDataModel.AgregatorPatientGender:
                return patient["resource"]["gender"]
                # TODO there is also this attribute
                #          {
                #     "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                #     "valueCode": "F"
                # },
            elif self.type_data_level == SeriesDataModel.AgregatorPatientMaritalStatus:
                return patient["resource"]["maritalStatus"]["coding"][0]["display"]
            elif self.type_data_level == SeriesDataModel.AgregatorPatientRace:
                for extension in patient["resource"]["extension"]:
                    if extension["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
                        return extension["extension"][0]["valueCoding"]["display"]
                return None
            elif self.type_data_level == SeriesDataModel.AgregatorPatientEthnicity:
                for extension in patient["resource"]["extension"]:
                    if extension["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                        return extension["extension"][0]["valueCoding"]["display"]
                return None

            elif self.type_data_level == SeriesDataModel.DataLevelInterval:
                # TODO also enforce resolution, and add that to the schema
                if self.measurement_source_name not in patient["dict_measurement"]:
                    if self.type_agregator == SeriesDataModel.AgregatorIntervalCountOccurance:
                        return 0
                    else:
                        return None

                list_measurement = patient["dict_measurement"][self.measurement_source_name]
                if self.type_agregator == SeriesDataModel.AgregatorIntervalFirstOccurance:
                    return list_measurement[0]["event_value"]
                if self.type_agregator == SeriesDataModel.AgregatorIntervalLastOccurance:
                    return list_measurement[-1]["event_value"]
                if self.type_agregator == SeriesDataModel.AgregatorIntervalCountOccurance:
                    return len(list_measurement)
                if self.type_agregator == SeriesDataModel.AgregatorIntervalMean:
                    list_measurement_value = [measurement["event_value"] for measurement in list_measurement]
                    return statistics.mean(list_measurement_value)

                else:
                    raise Exception(f"unkown type_agregator {self.type_agregator}")

            elif self.type_data_level == SeriesDataModel.DataLevelCategorical:
                if self.measurement_source_name not in patient["dict_measurement"]:
                    if self.type_agregator == SeriesDataModel.AgregatorCategoricalCountOccurance:
                        return 0
                    else:
                        return None

                list_measurement = patient["dict_measurement"][self.measurement_source_name]
                if self.type_agregator == SeriesDataModel.AgregatorCategoricalFirstOccurance:
                    return list_measurement[0]["event_value"]
                if self.type_agregator == SeriesDataModel.AgregatorCategoricalLastOccurance:
                    return list_measurement[-1]["event_value"]
                if self.type_agregator == SeriesDataModel.AgregatorCategoricalCountOccurance:
                    return len(list_measurement)
                if self.type_agregator == SeriesDataModel.AgregatorCategoricalMostFrequent:
                    raise NotImplementedError()  # TODO implement
                else:
                    raise Exception(f"unkown type_agregator {self.type_agregator}")

            else:
                raise Exception(f"unkown type_feature {self.type_data_level}")

        except Exception as exception:
            print(json.dumps(patient["resource"], indent=4, sort_keys=True))
            raise exception
        raise Exception(f"cannot return default")

    @staticmethod
    def create_unique(
        series_name: str,
        type_agregator: str = None,
    ) -> "SeriesDataModel":
        return SeriesDataModel(series_name, SeriesDataModel.DataLevelUnique, type_agregator=type_agregator)

    @staticmethod
    def create_numerical(
        series_name: str,
        resolution: Optional[float] = None,
        measurement_source_name: str = None,
        type_agregator: str = None,
        unit: str = "unitless",
    ) -> "SeriesDataModel":
        return SeriesDataModel(
            series_name,
            SeriesDataModel.DataLevelInterval,
            resolution=resolution,
            unit=unit,
            measurement_source_name=measurement_source_name,
            type_agregator=type_agregator,
        )

    @staticmethod
    def create_categorical(
        series_name: str, list_value: List[str], measurement_source_name: str = None, type_agregator: str = None
    ) -> "SeriesDataModel":
        return SeriesDataModel(
            series_name,
            SeriesDataModel.DataLevelCategorical,
            list_value=list_value,
            measurement_source_name=measurement_source_name,
            type_agregator=type_agregator,
        )

    def to_dict(self) -> Dict:
        dict = {}
        dict["__type__"] = "SeriesDataModel"
        dict["series_name"] = self.series_name
        dict["type_data_level"] = self.type_data_level
        dict["unit"] = self.unit
        dict["value_min"] = self.value_min
        dict["value_max"] = self.value_max
        dict["resolution"] = self.resolution
        dict["list_value"] = self.list_value
        dict["type_agregator"] = self.type_agregator
        dict["measurement_source_name"] = self.measurement_source_name
        return dict

    def from_dict(dict: Dict) -> "SeriesDataModel":
        return SeriesDataModel(
            dict["series_name"],
            dict["type_data_level"],
            unit=dict["unit"],
            value_min=dict["value_min"],
            value_max=dict["value_max"],
            resolution=dict["resolution"],
            list_value=dict["list_value"],
            type_agregator=dict["type_agregator"],
            measurement_source_name=dict["measurement_source_name"],
        )
