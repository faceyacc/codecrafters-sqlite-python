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

def read_varint_mem(buffer):
    """
    Reads a variable length integer from the buffer and returns it value.

    Args:
        buffer (bytes): The buffer to read from

    Returns:
        int: The variable length integer
    """
    val = 0
    buf_index = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F

    for _ in range(9):
        byte = buffer[buf_index]
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0:
            break
        buf_index += 1
    return val

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
        return None
    elif serial_type == 8:
        return 0
    elif serial_type == 9:
        return 1
    elif serial_type >= 12 and serial_type % 2 == 0: # check if BLOB type
        data_len = (serial_type - 12) // 2
        return database_file.read(data_len).decode()
    elif serial_type >= 13 and serial_type % 2 == 1: # check if TEXT type
        data_len = (serial_type - 13) // 2
        return database_file.read(data_len).decode()
    else:
        # print(f"Unknown serial type: {serial_type}") # TODO: add some error handling here.
        print(f"INVALID SERIAL TYPE {serial_type}")
        return None


def parse_cell(cell_pointer, database_file):
    """
    Parses a B-Tree Leaf Cell from a SQLite database file

    Args:
        cell_pointer (int): The pointer to the start of the cell in the database file
        database_file (file): The database file to read from

    Returns:
        List: A list of records in the cell
        int: The row id of the cell
    """
    # print(f"cell pointer: {cell_pointer}")
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

    records = []
    for serial_type in serial_types:
        records.append(parse_record(serial_type, database_file))

    return records, row_id
