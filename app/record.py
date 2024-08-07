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
        # print(f"Unknown serial type: {serial_type}") # TODO: add some error handling here.
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
    # print(f"cell pointer: {cell_pointer}")
    database_file.seek(cell_pointer)

    payload_size = read_varint(database_file)

    row_id = read_varint(database_file)
    # print(f'row_id: {row_id}')

    format_header_start = database_file.tell() # get current file position


    # Header size varint from the Record header
    format_header_size = read_varint(database_file)

    format_body_start = format_header_start + format_header_size
    # print(f'cell pointer:{cell_pointer}|row_id:{row_id}|format_header_start:{format_header_start}|format_header_size:{format_header_size}|format_body_start:{format_body_start}')

    # Serial Type Codes: One or more varints, each representing
    # the serial type of a column in the record.
    serial_types = []
    while database_file.tell() < format_body_start:
        serial_types.append(read_varint(database_file))


    records = []
    for serial_type in serial_types:
        records.append(parse_record(serial_type, database_file))

    # print(f'\nrecords: {records}')
    return records
