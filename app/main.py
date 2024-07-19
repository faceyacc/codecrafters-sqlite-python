import sys
import re

from dataclasses import dataclass
from typing import List

# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]


# Helper func to read int from bytes
def read_int(database_file, size):
    return int.from_bytes(database_file.read(size), byteorder="big")


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
def print_table_amount(database_file):

    # 1. Get page size from database file
    # 2. Get number of cells on page
    # 3. Calculate cell_pointers using right most pointer


    database_file.seek(16)
    page_size = read_int(database_file, 2)

    database_file.seek(103)
    number_of_cells = read_int(database_file, 2)

    database_file.seek(108)

    # Get right most pointer
    # right_most_pointer = read_int(database_file, 2)
    cell_pointers = [read_int(database_file, 2) for _ in range(number_of_cells)]
    records = [parse_cell(cell_pointer, database_file) for cell_pointer in cell_pointers]
    table_info = {record[2]: record[3] for record in records if record[2] != "sqlite_sequence"}


    table_page = table_info[table_name.encode('utf-8')]

    database_file.seek(((table_page - 1) * page_size) + 3)
    table_cell_amount = read_int(database_file, 2)
    return table_cell_amount


def read_varint(database_file):
    val = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F

    for _ in range(9):
        byte = read_int(database_file, 1)
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0:
            break
    return val


def parse_record(serial_type, database_file):
    if serial_type == 0:
        return None
    elif serial_type == 1:
        return read_int(database_file, 1)
    elif serial_type == 2:
        return read_int(database_file, 2)
    elif serial_type == 3:
        return read_int(database_file, 3)
    elif serial_type == 4:
        return read_int(database_file, 4)
    elif serial_type == 5:
        return read_int(database_file, 6)
    elif serial_type == 6:
        return read_int(database_file, 8)
    elif serial_type == 7:
        return read_int(database_file, 8)
    elif serial_type >= 12 and serial_type % 2 == 0:
        data_len = (serial_type - 12) // 2
        return database_file.read(data_len).decode()
    elif serial_type >= 13 and serial_type % 2 == 1:
        data_len = (serial_type - 13) // 2
        return database_file.read(data_len)
    else:
        print(f"Unknown serial type: {serial_type}")
        return None


def parse_cell(cell_pointer, database_file):
    # Go to right most pointer
    database_file.seek(cell_pointer)

    payload_size = read_varint(database_file)

    row_id = read_varint(database_file)

    format_header_start = database_file.tell() # get current file position

    format_header_size = read_varint(database_file)

    serial_types = []

    format_body_start = format_header_start + format_header_size

    while database_file.tell() < format_body_start:
        serial_types.append(read_varint(database_file))

    records = []
    for serial_type in serial_types:
        records.append(parse_record(serial_type, database_file))
    return records



if command == ".tables":
    with open(database_file_path, "rb") as database_file:

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Get list of names of tables in the database
        names = get_table_names(database_file)

        print(names)

elif command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:

        print("Logs from your program will appear here!")

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Read first two bytes and convert to integer
        page_size: int = int.from_bytes(database_file.read(2), byteorder="big")

        # Find number of rows in sqlite_shema table
        number_of_tables: int = sum(line.count(b"CREATE TABLE") for line in database_file)



        print(f"database page size: {page_size}")
        print(f"number of tables: {number_of_tables}")

# Check if command starts with "SELECT"
elif command.lower().startswith("select"):

    query = command.lower().split()

    # Get table name from SELECT command
    table_name = query[-1]

    with open(database_file_path, "rb") as database_file:
        table_amount = print_table_amount(database_file)
        print(table_amount)

else:
    print(f"Invalid command: {command}")
