from io import BufferedReader
import sys
import re
import sqlparse

from dataclasses import dataclass
from typing import List

from sqlparse.tokens import CTE

# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]



# def get_column_names(table_name):
#     column_names_regex = rb'\b(\w+)\s+text\b'
#     column_names = re.findall(column_names_regex, user_create_command)
#     column_names = [column_name.decode() for column_name in column_names]
#     table_name = table_name.decode()
#     print("\n".join(column_names))

# Helper func to read int from bytes
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


# Helper fucntion to do type and value encoding.
# Takes in a cell pointer and encodes if it is a BLOB, TEXT or it's value '
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
        # return database_file.read(data_len).decode()
        return database_file.read(data_len)
    elif serial_type >= 13 and serial_type % 2 == 1: # check if TEXT type
        data_len = (serial_type - 13) // 2
        return database_file.read(data_len)
    else:
        print(f"Unknown serial type: {serial_type}")
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
table = statement.split()[-1]




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

        database_file.seek(16)
        page_size = read_int(database_file, 2)

        # Read number of cells from page header
        database_file.seek(103)
        number_of_cells = read_int(database_file, 2)
        print(f'number of cells: {number_of_cells}')

        # Read right most pointer from page header
        database_file.seek(108)
        right_most_pointer = read_int(database_file, 2)
        print(f'right most pointer: {right_most_pointer}')

        cell_pointers = [right_most_pointer for _ in range(number_of_cells)]

        print(f'cell pointers: {cell_pointers}')

        # returns sqlite_master table
        records = [parse_cell(cell_pointer, database_file) for cell_pointer in cell_pointers]

        print(f'records: {records}')

        """
        LEFT OFF: right now if i print records, it will print the records of the table. I need a way
        to only print the records of the column that is being selected. Or atleast find a way to print blob
        data to debug further. I suspect that the blob data being printed is the acutal data of the
        column or entire table. (look at research form perplexity...)
        """
        table_info = {record[2]: record[3] for record in records if record[2] != "sqlite_sequence"}

        table_page = table_info[table_name]
        print(f'table page: {table_page}')


        # Go to page number of table
        database_file.seek(((table_page - 1) * page_size) + 3)
        table_cell_amount = read_int(database_file, 2)

        print(f"table cell amount: {table_cell_amount}")

        # Go to tables right most pointer
        database_file.seek(((table_page - 1) * page_size) + 8)
        table_right_most_pointer = read_int(database_file, 2)
        print(f'table right most pointer: {table_right_most_pointer}')

        table_cell_pointers = [table_right_most_pointer for _ in range(table_cell_amount)]
        print(f'table cell pointers: {table_cell_pointers}')

        table_records = [parse_cell(table_cell_pointers[0], database_file) for cell_pointer in table_cell_pointers]
        print(f'table records: {table_records}')


        # Get column name from SELECT command
        column_name = query[1].encode('utf-8')
        print(f'column name: {column_name}')
        print(f'table info: {table_info}')


        # Get column index from table_info
        # column_index = table_info[column_name]
        # print(f'column index: {column_index}')

        # Get the cell pointer for the first cell in the table
        # database_file.seek(((table_page - 1) * page_size) + 12)
        # table_cell_pointer = read_int(database_file, 2)

        # Parse the cell and get the records
        # table_records = parse_cell(table_cell_pointer, database_file)
        # print(f'records: {table_records}')









else:
    print(f"Invalid command: {command}")
