from enum import Enum, unique


@unique
class MessageType(Enum):
    NEW_NODE_SIGNUP = 1
    NEW_NODE_SIGNUP_RESPONSE = 2
    NEW_NODE_SIGNUP_COMPLETE = 3

    HEARTBEAT = 4

    ELECTION = 5
    ELECTION_RESPONSE = 6
    ELECTION_COMPLETE = 7

    DATA_SAVE = 8
    DATA_RETRIEVE = 9


def enum_to_json(value):
    if isinstance(value, Enum):
        return value.name  # or value.value to serialize as the integer value

    return value
