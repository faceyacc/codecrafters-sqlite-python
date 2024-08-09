from io import BufferedReader
from os import replace
import re
from .record import read_int, parse_cell, read_varint
from typing import Dict
import binascii
from .parser import *


def get_table_info(cell_pointers, database_file, table_name):
    for cell_pointer in cell_pointers:
        record, row_id = parse_cell(cell_pointer, database_file)
        if record[1] == table_name:
            return {
                "rootpage": record[3],
                "desc": parse(
                    record[4]
                    .lower()
                    .replace("(", "( ")
                    .replace(")", " )")
                    .replace(",", ", ")
                )
            }



def get_recs(start_offset, cells, database_file, tdesc, query_ref):
    records = []
    for cell_pointer in cells:
        cell, row_id = parse_cell(start_offset+cell_pointer, database_file)
        record = {}
        for column_name, column_value in zip(tdesc.col_names, cell):
            if column_name == "id":
                record[column_name] = column_value or row_id
            else:
                record[column_name] = column_value
        if query_ref.cond and query_ref.cond.col in record.keys():
            if query_ref.cond.comp(record[query_ref.cond.col]):
                continue
        records.append(list(record.values()))
    return records
