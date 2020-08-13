import unittest
import logging
from io import StringIO

from mongodb_backup.backup_service import MongoBackupService


class TestBackupService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.service: MongoBackupService = MongoBackupService(
            'mongodb://root:admin@localhost/admin')
        return super().setUpClass()

    def test_instance(self):
        self.assertIsInstance(self.service, MongoBackupService)

    def test_backup_collection(self):
        backup_dest = StringIO('', '\n')
        backup = self.service.backup_collection(
            'b2b_receiver', 'file_info', backup_dest)
        print(backup)
        backup_dest.seek(0, 0)
        backup_content = backup_dest.read()
        print(backup_content)

    def test_backup_database(self):
        backup_dest = StringIO('', '\n')
        backup = self.service.backup_database(
            'b2b_receiver', backup_dest)
        print(backup)
        backup_dest.seek(0, 0)
        backup_content = backup_dest.read()
        print(backup_content)

        self.service.restore_database('b2b_receiver', backup_dest)

    def test_backup_database_to_file(self):
        with open('backup.dat', 'w') as f:
            backup = self.service.backup_database('b2b_receiver', f)

        self.assertIsNotNone(backup)

    def test_restore_database_from_file(self):
        with open('backup.dat','r') as f:
            backup = self.service.restore_database('b2b_receiver',f)

        self.assertIsNotNone(backup)
