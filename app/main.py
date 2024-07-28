from io import BufferedReader
import sys
import re
import sqlparse

from dataclasses import dataclass
from typing import Dict, List

from sqlparse.tokens import CTE

# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]

def get_records(database_schema, database_file, page_size):
    """
    Returns a raw table records from the sqlite_master table.
    Args:
        database_schema (list): The sqlite_master table
        database_file (file): The database file to read from
        page_size (int): The size of the page in the database file
    Returns:
        bytes: The raw table records
    """
    table_info = {record[2]: record[3] for record in database_schema if record[2] != "sqlite_sequence"}


    # Get table page number
    table_page = table_info[table_name]

    # Go to page number of table
    database_file.seek(((table_page - 1) * page_size) + 3) # Offset 3 bytes from the start of the page to go to get number of cells on the page
    table_cell_amount = read_int(database_file, 2)

    # Go to tables right most pointer
    database_file.seek(((table_page - 1) * page_size) + 8) # Offset 8 bytes from the start of the page to get the right most pointer

    table_right_most_pointer = read_int(database_file, 2)

    # Get raw table records
    table_records = parse_cell(table_right_most_pointer, database_file)

    return table_records

def get_table(table_record, database_schema) -> Dict[str, str]:
    """
    Returns a dictionary representation of a table given a table record and database schema

    Args:
        table_record (bytes): The raw table records
        database_schema (list): The sqlite_master table

    Returns:
        dict[str, str]: A dictionary representation of the table
    """
    # clean the table records of None and empty bytes
    cleaned_table_records = []
    for record in table_records:
        if record == None:
            continue
        if record == b'':
            continue
        cleaned_table_records.append(record)
    cleaned_table_records = cleaned_table_records[-1]


    # Create dictionary representation of table
    column_names = get_column_names(database_schema)
    column_values = get_columns(cleaned_table_records)
    table: Dict[str, str] = {}
    try:
        if len(column_names) != len(column_values):
            raise ValueError("Length of column names and column values do not match.")
        for i in range(len(column_names)):
            table[column_names[i]] = column_values[i]
            # return table
    except IndexError as e:
        print(f"IndexError: {e}. Please ensure column names and column values have matching lengths.")
    except KeyError as e:
        print(f"KeyError: {e}. An invalid key was encountered.")
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return table


def text_map(string_values):
    """
    Returns a dictionary representation of a table given a list of string values

    Args:
        string_values (list): A list of string values

    Returns:
        dict: A dictionary representation of the table
    """
    text_map = {}

    for row in string_values:
        for column_index in range(len(row)):
            if column_index not in text_map:
                text_map[column_index] = []
            text_map[column_index].append(row[column_index])
    return text_map

def get_columns(record):
    """
    Returns the columns of a table given a record

    Args:
        record (bytes): The raw table records

    Returns:
        dict: A dictionary representation of the columns
    """
    column_names_regex = rb'(?:\d*[A-Za-z]+(?:[A-Za-z]+)+)'
    column_names = re.findall(column_names_regex, record)
    column_names = [column_name.decode() for column_name in column_names]

    # Split names by Upper case letters as a delimiter.
    names = [re.findall('[A-Z][a-z]*', name) for name in column_names]

    columns = text_map(names)
    return columns

def get_column_names(table_name):
    """
    Returns the column names of a table given a table name
    """
    columns = table_name[0][-1]
    table_names_regex = rb'\b(\w+)\s+text\b'
    columns = re.findall(table_names_regex, columns)
    column_names = [column_name.decode() for column_name in columns]
    return column_names

def read_int(database_file, size):
    """
    Reaturns an integer of an byte array given a database file and size

    Args:
        database_file (file): The database file to read from
        size (int): The number of bytes to read from the file

    Returns:
        int: The integer value of the byte array
    """
    return int.from_bytes(database_file.read(size), byteorder="big")


def get_table_names(database_file):
    """
    Returns a list of table names in the database file

    Args:
        database_file (file): The database file to read from

    Returns:
        str: A string of table names separated by a space
    """
    # Define pattern to match b'CREATE TABLE' statements using regex
    pattern = rb'CREATE TABLE (\w+)'

    f = database_file.read()

    # Find all tables in the database file using regex pattern
    tables = re.findall(pattern, f)

    tables = sorted([table.decode() for table in tables])
    tables = ' '.join(tables)

    return tables


def print_table_amount(database_file: BufferedReader) -> int:
    """
    Returns the number of rows in a table

    Args:
        database_file (file): The database file to read from

    Returns:
        int: The number of rows in the table
    """

    database_file.seek(16)
    page_size = read_int(database_file, 2)

    database_file.seek(103)
    number_of_cells = read_int(database_file, 2)

    database_file.seek(108)
    right_most_pointer = read_int(database_file, 2)


    cell_pointers = [right_most_pointer for _ in range(number_of_cells)]
    records = [parse_cell(cell_pointer, database_file) for cell_pointer in cell_pointers]

    table_info = {record[2]: record[3] for record in records if record[2] != "sqlite_sequence"}

    table_page = table_info[table_name]

    database_file.seek(((table_page - 1) * page_size) + 3)
    table_cell_amount = read_int(database_file, 2)

    return table_cell_amount


def read_varint(database_file):
    """
    Reads a variable length integer from the database file
    and returns it value.

    Args:
        database_file (file): The database file to read from

    Returns:
        int: The variable length integer
    """
    val = 0
    USE_NEXT_BYTE = 0x80 # Mask to check if the high-order bit is set.
    BITS_TO_USE = 0x7F # Mask to extract the lower 7 bits of each byte.

    for _ in range(9):
        byte = read_int(database_file, 1)
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0: # Check if the high-order bit is set
            break
    return val



def parse_record(serial_type, database_file):
    """
    Encodes the value of a record based on the serial type and returns it.

    Args:
        serial_type (int): The serial type of the record
        database_file (file): The database file to read from

    Returns:
        str: The encoded value of the record
    """
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
    elif serial_type >= 12 and serial_type % 2 == 0: # check if BLOB type
        data_len = (serial_type - 12) // 2
        return database_file.read(data_len)
    elif serial_type >= 13 and serial_type % 2 == 1: # check if TEXT type
        data_len = (serial_type - 13) // 2
        return database_file.read(data_len)
    else:
        print(f"Unknown serial type: {serial_type}") # TODO: add some error handling here.
        return None



def parse_cell(cell_pointer, database_file):
    """
    Parses a B-Tree Leaf Cell from a SQLite database file

    Args:
        cell_pointer (int): The pointer to the start of the cell in the database file
        database_file (file): The database file to read from

    Returns:
        List: A list of records in the cell
    """
    database_file.seek(cell_pointer)

    payload_size = read_varint(database_file)

    row_id = read_varint(database_file)

    format_header_start = database_file.tell() # get current file position

    # Header size varint from the Record header
    format_header_size = read_varint(database_file)

    format_body_start = format_header_start + format_header_size

    # Serial Type Codes: One or more varints, each representing
    # the serial type of a column in the record.
    serial_types = []
    while database_file.tell() < format_body_start:
        serial_types.append(read_varint(database_file))

    # print(f'serial_types: {serial_types}')
    records = []
    for serial_type in serial_types:
        records.append(parse_record(serial_type, database_file))

    # print(f'records: {records}')
    return records


statement = sqlparse.split(command)[0].lower()
table_name = statement.split()[-1]




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

# Handle reading the number of rows in a table
# Example: SELECT COUNT(*) FROM apples
elif command.lower().startswith("select") and 'count' in statement:

    query = command.lower().split()

    # Get table name from SELECT command
    table_name = query[-1].encode('utf-8')

    with open(database_file_path, "rb") as database_file:
        table_amount = print_table_amount(database_file)
        print(table_amount)

# Handle reading data from a single column
# Example: SELECT column FROM table
elif command.lower().startswith("select"):
    query = command.lower().split()

    # Get table name from SELECT command
    table_name = query[-1].encode('utf-8')

    with open(database_file_path, "rb") as database_file:

        # TODO 1: move this into a separate function and call it here.
        database_file.seek(16)
        page_size = read_int(database_file, 2)

        # Read number of cells from page header
        database_file.seek(103)
        number_of_cells = read_int(database_file, 2)
        # print(f'number of cells: {number_of_cells}')

        # Read right most pointer from page header
        database_file.seek(108)
        right_most_pointer = read_int(database_file, 2)

        cell_pointers = [right_most_pointer for _ in range(number_of_cells)]

        # returns sqlite_master (database_schema)
        database_schema = [parse_cell(cell_pointer, database_file) for cell_pointer in cell_pointers]
        # TODO 1: def get_database_schema(database_file)

        # Get table records
        table_records = get_records(database_schema, database_file, page_size)

        # clean the table records
        table = get_table(table_records, database_schema)

        # Get column name from SELECT command
        column_name = query[1]

        # Print out column values
        if column_name in table:
            print('\n'.join(table[column_name]))



else:
    print(f"Invalid command: {command}")
