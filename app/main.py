import sys
import re

from dataclasses import dataclass
from typing import List

# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]


# Helper function to get list of table names from database file
def get_table_names(database_file):
    # Define pattern to match b'CREATE TABLE' statements using regex
    pattern = rb'CREATE TABLE (\w+)'

    f = database_file.read()

    # Find all tables in the database file using regex pattern
    tables = re.findall(pattern, f)

    tables = sorted([table.decode() for table in tables])
    tables = ' '.join(tables)

    return tables


# Helper function to print b-tree page header
def print_b_tree_page_header(database_file):

    b_tree_type = int.from_bytes(database_file.read(1), byteorder="big")

    database_file.seek(1)
    first_freeblock = int.from_bytes(database_file.read(2), byteorder="big")

    # Read bytes at offset 3
    database_file.seek(3)
    number_of_cells = int.from_bytes(database_file.read(2), byteorder="big")

    database_file.seek(5)
    start_of_cell_content_area = int.from_bytes(database_file.read(2), byteorder="big")

    database_file.seek(8)
    right_pointer = int.from_bytes(database_file.read(4), byteorder="big")


    print(f"B-tree type -> {b_tree_type}\nFirst Free Block -> {first_freeblock}\nNumber of Cells on Page -> {number_of_cells}\nStart of Cell Content -> {start_of_cell_content_area}\nRight-most Pointer -> {right_pointer}\n")




# Check if command starts with "SELECT"
if command.startswith("select"):

    # Get table name from SELECT command
    table_name = command.split(" ")[3]

    with open(database_file_path, "rb") as database_file:

        # The 100-byte database file header is found only on page 1,
        # which is always a table b-tree page. All other b-tree pages
        # in the database file omit this 100-byte header.
        database_file.seek(100)
        print_b_tree_page_header(database_file)






if command == ".tables":
    with open(database_file_path, "rb") as database_file:

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Get list of names of tables in the database
        names = get_table_names(database_file)

        print(names)

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
