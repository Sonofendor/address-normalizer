from search.address import Address
from etl import connect_to_db
from typing import Iterable
from multiprocessing import Process, Queue, cpu_count
import os


def _process_address(address: str, connection) -> Address:
    address_object = Address(address, connection=connection)
    address_object.standardize(connection=connection)
    return address_object.to_dict()


def process_address_worker(task_queue: Queue, result_queue: Queue):
    connection = connect_to_db()
    while True:
        address = task_queue.get()
        if address is None:
            break
        address_object = Address(address, connection=connection)
        address_object.standardize(connection=connection)
        result = address_object.to_dict()
        result_queue.put(result)

    connection.close()

def process_batch(addresses: Iterable[str], processes: int = None):
    if processes is None or processes > cpu_count():
        processes = cpu_count()

    task_queue = Queue()
    result_queue = Queue()

    workers = []
    for _ in range(processes):
        p = Process(target=process_address_worker, args=(task_queue, result_queue))
        p.start()
        workers.append(p)

    for address in addresses:
        task_queue.put(address)

    for _ in range(processes):
        task_queue.put(None)

    results = []
    for _ in range(len(addresses)):
        results.append(result_queue.get())

    for p in workers:
        p.join()

    return results
