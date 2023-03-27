import json
import statistics

from sail_data_layer.series_data_model import SeriesDataModel


class Aggregator:
    AggregatorCsv = "AggregatorCsv"
    AggregatorComputed = "AggregatorComputed"
    AggregatorPatientGender = "AggregatorPatientGender"
    AggregatorPatientMaritalStatus = "AggregatorPatientMaritalStatus"
    AggregatorPatientRace = "AggregatorPatientRace"
    AggregatorPatientEthnicity = "AggregatorPatientEthnicity"
    AggregatorIntervalFirstOccurance = "AggregatorIntervalFirstOccurance"
    AggregatorIntervalLastOccurance = "AggregatorIntervalLastOccurance"
    AggregatorIntervalCountOccurance = "AggregatorIntervalCountOccurance"
    AggregatorIntervalMean = "AggregatorIntervalMean"
    AggregatorCategoricalFirstOccurance = "AggregatorCategoricalFirstOccurance"
    AggregatorCategoricalLastOccurance = "AggregatorCategoricalLastOccurance"
    AggregatorCategoricalCountOccurance = "AggregatorCategoricalCountOccurance"
    AggregatorCategoricalMostFrequent = "AggregatorCategoricalMostFrequent"

    def __init__(self, measurement_source_name: str, aggregator_type: str, series_data_model: SeriesDataModel) -> None:

        self.measurement_source_name = measurement_source_name
        self.aggregator_type = aggregator_type
        self.series_data_model = series_data_model

    def agregate(self, patient):
        try:
            # patient resource lookup
            # TODO refactor this
            if self.aggregator_type == Aggregator.AggregatorPatientGender:
                return patient["resource"]["gender"]
                # TODO there is also this attribute
                #          {
                #     "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                #     "valueCode": "F"
                # },
            elif self.aggregator_type == Aggregator.AggregatorPatientMaritalStatus:
                return patient["resource"]["maritalStatus"]["coding"][0]["display"]
            elif self.aggregator_type == Aggregator.AggregatorPatientRace:
                for extension in patient["resource"]["extension"]:
                    if extension["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
                        return extension["extension"][0]["valueCoding"]["display"]
                return None
            elif self.aggregator_type == Aggregator.AggregatorPatientEthnicity:
                for extension in patient["resource"]["extension"]:
                    if extension["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                        return extension["extension"][0]["valueCoding"]["display"]
                return None

            elif self.aggregator_type in [
                Aggregator.AggregatorIntervalCountOccurance,
                Aggregator.AggregatorIntervalFirstOccurance,
                Aggregator.AggregatorIntervalLastOccurance,
                Aggregator.AggregatorIntervalMean,
            ]:
                # TODO also enforce resolution, and add that to the schema
                if self.measurement_source_name not in patient["dict_measurement"]:
                    if self.aggregator_type == Aggregator.AggregatorIntervalCountOccurance:
                        return 0
                    else:
                        return None

                list_measurement = patient["dict_measurement"][self.measurement_source_name]
                if self.aggregator_type == Aggregator.AggregatorIntervalFirstOccurance:
                    return list_measurement[0]["event_value"]
                if self.aggregator_type == Aggregator.AggregatorIntervalLastOccurance:
                    return list_measurement[-1]["event_value"]
                if self.aggregator_type == Aggregator.AggregatorIntervalCountOccurance:
                    return len(list_measurement)
                if self.aggregator_type == Aggregator.AggregatorIntervalMean:
                    list_measurement_value = [measurement["event_value"] for measurement in list_measurement]
                    return statistics.mean(list_measurement_value)

                else:
                    raise Exception(f"unkown aggregator_type {self.aggregator_type}")

            elif self.aggregator_type in [
                Aggregator.AggregatorCategoricalFirstOccurance,
                Aggregator.AggregatorCategoricalLastOccurance,
                Aggregator.AggregatorCategoricalCountOccurance,
                Aggregator.AggregatorCategoricalMostFrequent,
            ]:
                if self.measurement_source_name not in patient["dict_measurement"]:
                    if self.aggregator_type == Aggregator.AggregatorCategoricalCountOccurance:
                        return 0
                    else:
                        return None

                list_measurement = patient["dict_measurement"][self.measurement_source_name]
                if self.aggregator_type == Aggregator.AggregatorCategoricalFirstOccurance:
                    return list_measurement[0]["event_value"]
                if self.aggregator_type == Aggregator.AggregatorCategoricalLastOccurance:
                    return list_measurement[-1]["event_value"]
                if self.aggregator_type == Aggregator.AggregatorCategoricalCountOccurance:
                    return len(list_measurement)
                if self.aggregator_type == Aggregator.AggregatorCategoricalMostFrequent:
                    raise NotImplementedError()  # TODO implement
                else:
                    raise Exception(f"unkown aggregator_type {self.aggregator_type}")

            else:
                raise Exception(f"unkown aggregator_type {self.aggregator_type}")

        except Exception as exception:
            print(json.dumps(patient["resource"], indent=4, sort_keys=True))
            raise exception
        raise Exception(f"cannot return default")
