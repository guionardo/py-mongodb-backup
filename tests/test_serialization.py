import unittest
import datetime

from mongodb_backup.serialization_service import serialize_data, deserialize_data


class TestSerialization(unittest.TestCase):

    def test_serialization(self):
        obj = {
            "name": "Guionardo Furlan",
            "today": datetime.datetime.today(),
            "age": 43,
            "height": 1.72,
            "developer": True
        }
        serialized_data = serialize_data(obj)

        unserialized_data = deserialize_data(serialized_data)

        self.assertDictEqual(obj, unserialized_data)
