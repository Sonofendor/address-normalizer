from search.address import Address
from etl import connect_to_db
from typing import Dict


def search_address(address: str, connection) -> Dict:
    address_object = Address(address, connection=connection)
    address_object.standardize(connection=connection)
    return address_object.to_dict()
