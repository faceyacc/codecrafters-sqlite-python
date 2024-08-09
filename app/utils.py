from io import BufferedReader
from os import replace
import re
from .record import read_int, parse_cell, read_varint
from typing import Dict
import binascii
from .parser import *

# Class to indicate the b-tree page type
class PageType:
    # InteriorIndex = 0x02
    InteriorTable = 0x05
    # LeafIndex = 0x0A
    LeafTable = 0x0D

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


def travel_pages(pg_num, pgsz, db_file, tdesc, query_ref):
    db_file.seek(pg_num)
    page_type = read_int(db_file, 1)
    db_file.seek(pg_num + 3)
    cell_amt = read_int(db_file, 2)
    db_file.seek(pg_num + (12 if page_type & 8 == 0 else 8))
    cell_ptrs = [read_int(db_file, 2) for _ in range(cell_amt)]

    if page_type == PageType.InteriorTable:
        records = []
        for c_ptr in cell_ptrs:
            db_file.seek(pg_num + c_ptr)
            page_num = read_int(db_file, 4)
            key = read_varint(db_file)
            records.extend(
                travel_pages((page_num - 1) * pgsz, pgsz, db_file, tdesc, query_ref) # pyright: ignore
            )
        return records
    elif page_type == PageType.LeafTable:
        return get_recs(pg_num, cell_ptrs, db_file, tdesc, query_ref)
