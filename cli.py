from cli.parser import setup_parser
import argparse
import sys
from cli.logger import setup_logger


def main():
    parser = setup_parser()
    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()
    func = args.func
    if not func:
        parser.print_help(sys.stderr)
        sys.exit(1)
    setup_logger(args.log_level)

    if not func(args):
        sys.exit(2)


if __name__ == "__main__":
    main()
