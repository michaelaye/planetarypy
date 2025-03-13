"""Console script for planetarypy."""

import argparse
import sys


def main(args=None):
    """Console script for planetarypy.

    Args:
        args: List of command line arguments. If None, sys.argv[1:] is used.

    Returns:
        int: Return code
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("_", nargs="*")
    args = parser.parse_args(args)

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into planetarypy.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
