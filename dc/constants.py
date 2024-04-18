from enum import Enum, unique


@unique
class MessageType(Enum):
    NEW_NODE_SIGNUP = 1

    DATA_SAVE = 2
    DATA_RETRIEVE = 3


def enum_to_json(value):
    if isinstance(value, Enum):
        return value.name  # or value.value to serialize as the integer value

    return value
