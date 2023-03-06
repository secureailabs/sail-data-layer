import json
import os
import shutil
import tempfile
import zipfile
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import dateutil.parser

from sail_data_layer.base_dataset_serializer import BaseDatasetSerializer
from sail_data_layer.longitudinal_dataset import LongitudinalDataset
from sail_data_layer.longitudinal_dataset_data_model import \
    LongitudinalDatasetDataModel


class Fhirv1DatasetSerializer(BaseDatasetSerializer):
    # A general note on the ZipFile package: The initializer of ZipFile takes a fourth argument called allowZip64.
    # It’s a Boolean argument that tells ZipFile to create ZIP files with the .zip64 extension for files larger than 4 GB.

    # zipfile.ZIP_DEFLATED	Deflate	zlib
    # zipfile.ZIP_BZIP2	    Bzip2	bz2
    # zipfile.ZIP_LZMA	    LZMA	lzma
    def __init__(self) -> None:
        super().__init__("fhirv1")

    def read_dataset(self, dataset_id: str) -> LongitudinalDataset:
        return self.read_dataset_for_path(os.path.join(self.path_dir_dataset_store, dataset_id))

    def read_dataset_for_path(self, path_dir_dataset) -> LongitudinalDataset:
        list_patient = []
        path_file_header = os.path.join(path_dir_dataset, "dataset_header.json")
        path_file_data_model = os.path.join(path_dir_dataset, "data_model.zip")
        path_file_data_content = os.path.join(path_dir_dataset, "data_content.zip")

        # read header
        with open(path_file_header, "r") as file:
            dataset_header = json.load(file)
        if self.dataset_packaging_format != dataset_header["dataset_packaging_format"]:
            raise Exception()

        # data model
        with ZipFile(path_file_data_model) as archive_data_model:
            # header_dataset = json.loads(archive_data_model.read("data"))
            data_model = LongitudinalDatasetDataModel.from_dict({})

        # data content
        with ZipFile(path_file_data_content) as archive_data_content:
            for name_file in archive_data_content.namelist():
                if ".json" not in name_file:
                    raise Exception(f"Non json file in fhirv1 archive: {name_file}")
                patient = json.loads(archive_data_content.read(name_file))
                list_patient.append(self.process_patient(patient))

        data_federation_id = dataset_header["data_federation_id"]
        data_federation_name = dataset_header["data_federation_name"]
        dataset_id = dataset_header["dataset_id"]
        dataset_name = dataset_header["dataset_id"]
        return LongitudinalDataset(
            data_federation_id, data_federation_name, dataset_id, dataset_name, data_model, list_patient
        )

    def process_patient(self, dict_patient):
        # step 1 find the patient resource
        patient = {}
        patient["resource"] = ""
        patient["dict_measurement"] = {}
        for entry in dict_patient["entry"]:
            # TODO packaging, check that there is only one patient resource per file and that all events relate to that patient
            if "Patient" == entry["resource"]["resourceType"]:
                patient["resource"] = entry["resource"]

        list_event = []
        for entry in dict_patient["entry"]:
            resource = entry["resource"]
            list_event.extend(self.parse_list_event(resource))

        # print(f"{name_file} {len(dict)} {dict.keys()}")
        for event in list_event:
            measurement = event["event_type"]  # TODO rename?
            if measurement not in patient["dict_measurement"]:
                patient["dict_measurement"][measurement] = []
            patient["dict_measurement"][measurement].append(event)

        # sort measurements by date
        for measurement in patient["dict_measurement"]:
            patient["dict_measurement"][measurement] = sorted(
                patient["dict_measurement"][measurement], key=lambda measurement_val: measurement_val["datetime_object"]
            )

        return patient

    def parse_list_event(self, resource):
        list_event = []
        resource_type = resource["resourceType"]
        try:
            if resource_type == "Encounter":
                event_type = resource_type + ":" + resource["type"][0]["coding"][0]["display"]
                event_value = resource["status"]
                datetime_object = dateutil.parser.isoparse(resource["period"]["start"])

            elif resource_type == "Condition":
                event_type = resource_type + ":" + resource["code"]["coding"][0]["display"]
                event_value = resource["verificationStatus"]["coding"][0]["code"]
                datetime_object = dateutil.parser.isoparse(resource["recordedDate"])
                list_event.append(
                    {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                )

            elif resource_type == "Observation":
                datetime_object = dateutil.parser.isoparse(resource["effectiveDateTime"])
                if "component" in resource:
                    for component in resource["component"]:
                        event_type = resource_type + ":" + component["code"]["coding"][0]["display"]
                        event_value = component["valueQuantity"]["value"]
                        list_event.append(
                            {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                        )
                else:
                    event_type = resource_type + ":" + resource["code"]["coding"][0]["display"]
                    if "valueQuantity" in resource:
                        event_value = resource["valueQuantity"]["value"]
                    elif "valueString" in resource:
                        event_value = resource["valueString"]
                    else:
                        event_value = resource["valueCodeableConcept"]["coding"][0]["display"]

                    # TODO add unit?

                    list_event.append(
                        {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                    )

            elif resource_type == "Procedure":
                event_type = resource_type + ":" + resource["code"]["coding"][0]["display"]
                event_value = resource["status"]
                datetime_object = dateutil.parser.isoparse(resource["performedPeriod"]["start"])

                list_event.append(
                    {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                )

            elif resource_type == "MedicationRequest":
                event_type = resource_type + ":" + resource["medicationCodeableConcept"]["coding"][0]["display"]
                event_value = resource["status"]
                datetime_object = dateutil.parser.isoparse(resource["authoredOn"])
                list_event.append(
                    {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                )

            elif resource_type == "Immunization":
                event_type = resource_type + ":" + resource["vaccineCode"]["coding"][0]["display"]
                event_value = resource["status"]
                datetime_object = dateutil.parser.isoparse(resource["occurrenceDateTime"])
                list_event.append(
                    {"event_type": event_type, "event_value": event_value, "datetime_object": datetime_object}
                )

        except Exception as exception:
            print(json.dumps(resource, indent=4, sort_keys=True))
            print(resource_type)
            raise exception

        return list_event
