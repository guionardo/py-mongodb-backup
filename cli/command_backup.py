from mongodb_backup import MongoBackupService
from logging import getLogger
import os
logger = getLogger('backup')


def command_backup(args):
    backup_file = os.path.realpath(args.backup_file)
    logger.info('START: %s -> %s', args.mongodb_uri, backup_file)
    service = MongoBackupService(args.mongodb_uri)
    try:
        
        obj_count, written = service.backup_database(args.database_name, backup_file)

        logger.info('End backup: objects = %s, data size = %s',
                    obj_count, written)
        return True
    except Exception as exc:
        logger.error(str(exc))
    return False
