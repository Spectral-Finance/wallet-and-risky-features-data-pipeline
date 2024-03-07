from os import path
from typing import Any, MutableMapping

from pytest import fixture
import toml


@fixture(scope="function")
def read_map_db() -> MutableMapping[str, Any]:
    """Function to read map db toml file

    Returns:
        MutableMapping[str, Any]: map_db

    """
    with open(path.join(path.dirname(path.abspath(__file__)), "../config/map_db.toml")) as toml_file:
        map_db = toml.load(toml_file)
    return map_db
