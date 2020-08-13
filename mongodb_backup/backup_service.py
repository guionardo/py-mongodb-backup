import io
import uuid
import logging
import time
from typing import Union

from pymongo import DeleteMany, InsertOne, MongoClient
from pymongo.collection import (BulkWriteError, BulkWriteResult, Collection,
                                IndexModel)
from pymongo.database import Database

from .exceptions import MongoDBBackupException
from .serialization_service import deserialize_data, serialize_data


class MongoBackupService:

    DATABASE = '#D'
    END_DATABASE = '#d'
    COLLECTION = '#C'
    END_COLLECTION = '#c'
    INDEX = '#I'
    DATA = '#X'

    LOG = logging.getLogger(__name__)

    def __init__(self, connection_string: str):
        self._connection_string = connection_string
        self._client: MongoClient = MongoClient(connection_string)

    def _get_database(self, database_name: Union[str, Database]) -> Database:
        if isinstance(database_name, str):
            if database_name not in self._client.list_database_names():
                raise MongoDBBackupException('Database {0} not found in {1}',
                                             database_name, self._connection_string)

            database = self._client.get_database(database_name)
        elif isinstance(database_name, Database):
            database = database_name
        else:
            raise MongoDBBackupException(
                "Argument error for database. Expected str or Database")
        return database

    def _get_file_object(self, file_object: Union[str, io.FileIO], writable: bool = True) -> io.FileIO:
        if isinstance(file_object, str):
            file_object = open(file_object, 'a' if writable else 'r')
        elif not isinstance(file_object, io.IOBase):
            raise MongoDBBackupException(
                "Invalid file_object {0}", file_object)
        if writable and not file_object.writable():
            raise MongoDBBackupException(
                "File_object is not writable {0}", file_object)
        elif not writable and not file_object.readable():
            raise MongoDBBackupException(
                "File_object is not readable {0}", file_object)

        return file_object

    def backup_database(self, database_name: str, file_object: Union[str, io.FileIO]):
        database = self._get_database(database_name)
        file_object = self._get_file_object(file_object)
        written = file_object.write(f'{self.DATABASE} {database.name}\n')

        for collection_name in database.list_collection_names():
            written += self.backup_collection(database,
                                              collection_name, file_object)
        written += file_object.write(f'{self.END_DATABASE} {database.name}\n')
        return written

    def backup_collection(self, database_name: str, collection_name: str, file_object: io.FileIO) -> int:
        file_object = self._get_file_object(file_object)

        database = self._get_database(database_name)

        if collection_name not in database.list_collection_names():
            raise MongoDBBackupException('Collection {0} not found in {1}/{2}',
                                         collection_name, self._connection_string, database.name)
        collection: Collection = database.get_collection(collection_name)
        written = file_object.write(
            f'{self.COLLECTION} {collection.full_name}\n')
        indexes = collection.index_information()
        written += file_object.write(
            f'{self.INDEX} {serialize_data(indexes)}\n')

        for data in collection.find():
            written += file_object.write(
                f'{self.DATA} {serialize_data(data)}\n')
        written += file_object.write(
            f'{self.END_COLLECTION} {collection.full_name}\n')

        return written

    def restore_database(self, database_name: str, file_object: io.FileIO) -> bool:
        database = self._get_database(database_name)
        file_object = self._get_file_object(file_object, writable=False)
        file_object.seek(0, 0)
        line = file_object.readline().strip('\n')

        if not line.startswith(self.DATABASE) or \
                line[len(self.DATABASE)+1:] != database.name:
            raise MongoDBBackupException(
                'Invalid header for database: {0}', line)

        restored = 0
        while restored > -1:
            restored = self.restore_collection(database, file_object)

        return True

    def restore_collection(self, database_name: str, file_object: io.FileIO) -> int:
        database = self._get_database(database_name)
        file_object = self._get_file_object(file_object, writable=False)
        line = file_object.readline().strip('\n')
        if not line or line.startswith(self.END_DATABASE):
            return -1

        if not line.startswith(self.COLLECTION):
            raise MongoDBBackupException(
                'Invalid header for collection: {0}', line)

        collection_name = line[len(self.DATABASE)+1:].split('.')[-1]
        if not collection_name:
            raise MongoDBBackupException(
                'Invalid header for collection: name not informed: {0}', line)

        line = file_object.readline().strip('\n')
        if not line.startswith(self.INDEX):
            raise MongoDBBackupException(
                'Invalid index header for collection {0}: {1}', collection_name, line)

        t0 = time.time()

        backup_collection = self._create_backup_collection(
            database, collection_name)

        collection = database.get_collection(collection_name)

        self._create_indexes(line[len(self.INDEX)+1:], collection)

        collection.delete_many({})
        requests = []
        written = 0
        while line:
            line = file_object.readline().strip('\n')
            if line.startswith(self.DATA):
                data = deserialize_data(line[len(self.DATA)+1:])
                requests.append(InsertOne(data))
                if len(requests) > 50:
                    written += self._bulk_write(requests, collection)
            elif line.startswith(self.END_COLLECTION):
                line = None
            else:
                self.LOG.error('UNEXPECTED DATA LINE %s', line)
                line = None

        written += self._bulk_write(requests, collection)

        if written == len(requests):
            self._remove_backup_collection(
                database, backup_collection, collection_name)

            self.LOG.info('Restored [%s.%s] %s objects in %s ms',
                          database.name,
                          collection_name,
                          written,
                          int((time.time()-t0)*1000))
        return written

    def _create_indexes(self, index_line, collection: Collection):
        indexes = deserialize_data(index_line)
        index_models = []
        index_desc = []
        for index in indexes:
            index_args = indexes[index]
            keys = [(key, int(order)) for (key, order) in index_args['key']]
            index_args.pop('key')
            index_args['name'] = index
            index_desc.append('{0} = {1} ({2})'.format(
                index, keys, index_args))
            index_models.append(IndexModel(keys, **index_args))

        try:
            result = collection.create_indexes(index_models)
            self.LOG.debug('CREATED INDEXES: %s = %s',
                           collection.name, [s for s in zip(index_desc, result)])
        except Exception as exc:
            self.LOG.error('ERROR ON CREATING INDEXES: %s', exc)

    def _bulk_write(self, requests, collection: Collection) -> int:
        if requests:
            try:

                t0 = time.time()
                result: BulkWriteResult = collection.bulk_write(
                    requests, ordered=False)
                self.LOG.debug('WRITE [%s] %s EVENTS IN %s ms: %s',
                               collection.name,
                               len(requests),
                               int((time.time()-t0)*1000),
                               result.bulk_api_result)
                return result.inserted_count
            except BulkWriteError as exc:
                self.LOG.error("Failed to write data: %s", exc.details)

        return 0

    def _create_backup_collection(self, database: Database, collection_name) -> str:
        """ Rename collection to prevent data loss on error

        Returns:    None if the collection not exists or error
                    new name of collection
        """
        if collection_name not in database.list_collection_names():
            return None

        try:
            new_name = '_'+collection_name+'_'+str(uuid.uuid4())
            collection = database.get_collection(collection_name)
            result = collection.rename(new_name)
            self.LOG.info('Collection %s.%s renamed to %s.%s: %s',
                          database.name,
                          collection_name,
                          database.name,
                          new_name,
                          result)
            return new_name
        except Exception as exc:
            self.LOG.warning('Cannot rename collection %s.%s: %s',
                             database.name,
                             collection_name,
                             exc)
        return None

    def _remove_backup_collection(self, database: Database, backup_collection: str, collection_name: str):
        if not backup_collection:
            return

        try:
            database.drop_collection(backup_collection)
            self.LOG.info('Removed backup collection for %s.%s: %s',
                          database.name,
                          collection_name,
                          backup_collection)
        except Exception as exc:
            self.LOG.warning('Error when removing backup collection %s.%s: %s',
                             database.name,
                             backup_collection,
                             str(exc))
