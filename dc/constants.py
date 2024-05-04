from enum import Enum, unique


@unique
class MessageType(Enum):
    NEW_NODE_SIGNUP = 1
    NEW_NODE_SIGNUP_RESPONSE = 2

    DATA_SAVE = 3
    DATA_RETRIEVE = 4


def enum_to_json(value):
    if isinstance(value, Enum):
        return value.name  # or value.value to serialize as the integer value

    return value
