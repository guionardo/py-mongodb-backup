import base64
import pickle
from datetime import datetime

from orjson import dumps, loads

_encoder = base64.a85encode
_decoder = base64.a85decode


def serialize_data(data: dict) -> str:
    return _encoder(pickle.dumps(data)).decode(encoding='ascii')


def deserialize_data(serialized: str) -> dict:
    return pickle.loads(_decoder(serialized))
