class MongoDBBackupException(Exception):

    def __init__(self, message, *args, **kwargs):   # pragma: nocover
        super().__init__(message.format(*args, **kwargs))
