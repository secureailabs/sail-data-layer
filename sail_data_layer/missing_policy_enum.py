from enum import Enum


class MissingPolicyEnum(str, Enum):
    MissingPolicyPropagateAddColumn = "MissingPolicyPropagateAddColumn"
    MissingPolicyRaiseException = "MissingPolicyRaiseException"

    def __str__(self) -> str:
        return str(self.value)

    @staticmethod
    def parse_str(value: str):
        if value == MissingPolicyEnum.MissingPolicyPropagateAddColumn:
            return MissingPolicyEnum.MissingPolicyPropagateAddColumn
        elif value == MissingPolicyEnum.MissingPolicyRaiseException:
            return MissingPolicyEnum.MissingPolicyRaiseException
        else:
            raise ValueError(f"Illegal enum value: {value}")
