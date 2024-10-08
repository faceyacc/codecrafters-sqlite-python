import sys
import re
import sqlparse

from dataclasses import dataclass
from typing import Dict, List

from sqlparse.tokens import CTE

from .record import read_int, read_varint, parse_cell, parse_record
from .utils import *
from .parser import *

# Get commands from command line
database_file_path = sys.argv[1]
command = sys.argv[2]

statement = sqlparse.split(command)[0].lower()
table_name = statement.split()[-1]



if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:

        print("Logs from your program will appear here!")

        database_file.seek(16)  # Skip the first 16 bytes of the header

        # Read first two bytes and convert to integer
        page_size: int = int.from_bytes(database_file.read(2), byteorder="big")

        database_file.seek(103)
        table_amount = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}\nnumber of tables: {table_amount}")


elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(103)
        cell_amount = read_int(database_file, 2)
        database_file.seek(108)
        cell_pointers = [read_int(database_file, 2) for _ in range(cell_amount)]
        records = [parse_cell(cell_pointer, database_file)[0] for cell_pointer in cell_pointers]
        table_names = [record[2] for record in records if record[2] != "sqlite_sequence"]

        print(*table_names)

elif command.lower().startswith("select"):

    p_query = parse(command.lower())

    with open(database_file_path, "rb") as database_file:

        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")

        # Read number of cells from page header
        database_file.seek(103)
        cell_amount = read_int(database_file, 2)


        # Read right most pointer from page header
        database_file.seek(108)
        cell_pointers = [read_int(database_file, 2) for _ in range(cell_amount)]

        table_info = get_table_info(cell_pointers, database_file, p_query.table)
        page_offset = (table_info["rootpage"] - 1) * page_size # type: ignore

        records = travel_pages(page_offset, page_size, database_file, table_info["desc"], p_query) # type: ignore

        if p_query.count_cols:
            print(len(records)) # type: ignore
        else:
            column_idxs = []
            for column in p_query.col_names:
                column_idxs.append(table_info["desc"].col_names.index(column)) # type: ignore
            results = [[r[col_idx] for col_idx in column_idxs] for r in records if r] # type: ignore

            for result in results:
                print(*result, sep="|")

else:
    print(f"Invalid command: {command}")
