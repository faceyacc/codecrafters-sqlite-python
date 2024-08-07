import sys
import re
import sqlparse

from dataclasses import dataclass
from typing import Dict, List

from sqlparse.tokens import CTE

from .record import read_int, read_varint, parse_cell, parse_record
from .filter import get_index, where_filter
from .utils import *




# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]


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
        table_amount = print_table_amount(database_file, table_name)
        print(table_amount)

# Handle reading data from columns
# Example: SELECT column FROM table
elif command.lower().startswith("select"):
    query = command.lower().split()

    # Get table name from SELECT command
    table_name = query[query.index("from") + 1].encode('utf-8')

    with open(database_file_path, "rb") as database_file:

        database_schema, page_size = get_database_schema(database_file)

        # print(f'schema: {database_schema}')

        # Get table records
        table_records = get_records(database_schema, database_file, page_size, table_name)

        # print(f'table records: {table_records}')

        # clean the table records
        table = get_table(table_records, database_schema)

        clean_large_table(table_records)



        # Handle reading data from multiple columns
        # Example: SELECT column1, column2, columnN FROM table
        if len(query) > 4:
            columns = query[1:-2]

            columns = [column.rstrip(',') for column in columns] # Remove comma from columns

            multi_column = []
            for column in columns:
                if column in table:
                    multi_column.append((table[column]))

            # Handle reading data from multiple columns with a WHERE clause
            # Example: SELECT column1, column2, columnN FROM table WHERE column = value
            if 'where' in query:
                where_clause = query[-1].strip("'").capitalize()
                index = get_index(multi_column, where_clause)
                row_values = where_filter(multi_column, index, where_clause)
                print(row_values)

            # Handle reading data from multiple columns
            # Example: SELECT column1, column2, columnN FROM table
            else:
                for row in zip(*multi_column):
                    print('|'.join(value for value in row))


        # Handle reading data from a single column
        # Example: SELECT column FROM table
        else:
            # Get column name from SELECT command
            column_name = query[1]


            # Print out column values
            if column_name in table:
                print('\n'.join(table[column_name]))




else:
    print(f"Invalid command: {command}")
