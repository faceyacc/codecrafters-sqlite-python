import sys

from dataclasses import dataclass
from loguru import logger


# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:

        logger.debug("Logs from your program will appear here!")

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Read first two bytes and convert to integer
        page_size = int.from_bytes(database_file.read(2), byteorder="big")

        logger.debug(f"database page size: {page_size}")
else:
    logger.debug(f"Invalid command: {command}")
