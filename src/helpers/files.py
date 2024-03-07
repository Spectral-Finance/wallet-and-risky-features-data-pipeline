def read_sql_file(file_path: str) -> str:
    """Reads the sql file and returns the query

    Args:
        file_path (str): Path of the sql file

    Returns:
        str: Query
    """

    try:
        with open(file_path, "r") as file:
            query = file.read()
    except Exception as error:
        raise Exception(f"Error while reading sql file: {error}")

    return query
