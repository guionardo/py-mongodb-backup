import argparse
import logging

from pathvalidate.argparse import validate_filename_arg

from mongodb_backup import __description__, __version__

from .command_backup import command_backup


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__description__)

    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument(
        '-l', '--log-level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info',
        dest='log_level')
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers()

    setup_backup_parser(subparsers)
    setup_restore_parser(subparsers)

    return parser


def setup_backup_parser(subparsers):
    parser_backup = subparsers.add_parser(
        'backup', help='Create backup file from MongoDB database')
    parser_backup.set_defaults(func=command_backup)
    setup_common_arguments(parser_backup)


def setup_restore_parser(subparsers):
    parser_restore = subparsers.add_parser(
        'restore', help='Restore MongoDB database from backup file')
    setup_common_arguments(parser_restore)


def setup_common_arguments(parser):
    parser.add_argument('-f', '--backup-file',
                        help='Backup file',
                        dest='backup_file',
                        type=validate_filename_arg,
                        required=True)

    parser.add_argument('-u', '--mongodb-uri',
                        help='MongoDB URI (i.ex: mongodb://username:password@host:27017/admin)',
                        dest='mongodb_uri',
                        required=True)

    parser.add_argument('-d', '--database',
                        help='Database name',
                        dest='database_name',
                        required=True)
