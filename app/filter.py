def get_index(columns, index):
    """
    Returns the index of a table given a table and index

    Args:
        table (list): The columsn to search
        index (int): The WHERE clause to search for

    Returns:
        str: The index of the WHERE clause
    """
    for column in columns:
        if index in column:
            return column.index(index)

def where_filter(table, index, where_clause):
    """
    Returns a filtered table given a table, index, and WHERE clause

    Args:
        table (list): The table to filter
        index (int): The index of the table to filter
        where_clause (str): The WHERE clause to filter by

    Returns:
        list: The filtered table
    """
    rows = []
    for row in table:
        rows.append(row[index])

    # Removes duplicates while perserving order
    seen = set()

    if where_clause in rows:
        rows.pop()

    rows = [row for row in rows if not (row in seen or seen.add(row))]

    return '|'.join(rows)
