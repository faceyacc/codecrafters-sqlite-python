from io import BufferedReader
import re
from .record import read_int, parse_cell
from typing import Dict


def clean_large_table(table_record):

    cleaned_table_records = []

    for record in table_record:
        if record == None:
            continue
        if record == b'':
            continue
        cleaned_table_records.append(record)


    print(cleaned_table_records)


def clean_tables(table_record):
    # clean the table records of None and empty bytes
    cleaned_table_records = []

    for record in table_record:
        if record == None:
            continue
        if record == b'':
            continue
        cleaned_table_records.append(record)
    cleaned_table_records = cleaned_table_records[-1]

    return cleaned_table_records



def print_table_amount(database_file: BufferedReader, table_name) -> int:
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


def get_database_schema(database_file):
    """
    Returns the database schema from the sqlite_master table

    Args:
        database_file (file): The database file to read from

    Returns:
        list: The database schema
        page_size: The size of the page in the database file
    """
    database_file.seek(16)
    page_size = read_int(database_file, 2)

    # Note: b-tree page type is always 0x0d (leaf table b-tree)

    # Read number of cells from page header
    database_file.seek(103)
    number_of_cells = read_int(database_file, 2)


    # Read right most pointer from page header
    database_file.seek(108)
    right_most_pointer = read_int(database_file, 2)

    cell_pointers = [right_most_pointer for _ in range(number_of_cells)]

    print(f'length of cell_pointers: {len(cell_pointers)}')

    # returns sqlite_master (database_schema)
    database_schema = [parse_cell(cell_pointer, database_file) for cell_pointer in cell_pointers]

    return database_schema, page_size

def get_records(database_schema, database_file, page_size, table_name):
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

    cleaned_table_records = clean_tables(table_record)

    # Create dictionary representation of table
    column_names = get_column_names(database_schema)
    column_values = get_columns(cleaned_table_records)
    table: Dict[str, str] = {}


    try:
        for i in range(len(column_names)):
            table[column_names[i]] = column_values[i]
    except KeyError as e:
        print(f"KeyError: {e}. An invalid key was encountered.")

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

    if isinstance(record, int):
        record = str(record).encode('utf-8')


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
