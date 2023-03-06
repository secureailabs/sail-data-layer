class BaseDataset:
    def __init__(
        self,
        dataset_federation_id: str,
        dataset_federation_name: str,
        dataset_id: str,
        dataset_name: str,
    ) -> None:
        self.__dataset_federation_id = dataset_federation_id
        self.__dataset_federation_name = dataset_federation_name
        self.__dataset_id = dataset_id
        self.__dataset_name = dataset_name



    # property section start
    @property
    def dataset_federation_id(self) -> str:
        return self.__dataset_federation_id

    @property
    def dataset_federation_name(self) -> str:
        return self.__dataset_federation_name

    @property
    def dataset_id(self) -> str:
        return self.__dataset_id

    @property
    def dataset_name(self) -> str:
        return self.__dataset_name

    # property section end