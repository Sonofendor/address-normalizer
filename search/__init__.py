from search.address import Address
from typing import Iterable
from multiprocessing import Pool, cpu_count, freeze_support

def process_address(address: str) -> Address:
    processed_address = Address(address)
    return processed_address


def search_in_kladr(address: Address):
    return address.search_in_kladr()[0], address.raw_address, address.search_string


def process_batch(addresses: Iterable[str], processes: int = None):
    freeze_support()
    if processes > cpu_count():
        processes = cpu_count()
    with Pool(processes) as pool:
        processed_addresses = pool.map(search_in_kladr, addresses)
    return processed_addresses
