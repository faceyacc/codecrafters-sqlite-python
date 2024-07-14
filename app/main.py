import sys

from dataclasses import dataclass


# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:

        print("Logs from your program will appear here!")

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Read first two bytes and convert to integer
        page_size: int = int.from_bytes(database_file.read(2), byteorder="big")

        # Find number of rows in sqlite_shema table
        number_of_tables: int = sum(line.count(b"CREATE TABLE") for line in database_file)





        print(f"database page size: {page_size}")
        print(f"number of tables: {number_of_tables}")
else:
    print(f"Invalid command: {command}")
