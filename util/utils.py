import logging
import time


def measure(comment, method, *args):
    start_time = time.perf_counter()
    result = method(*args)
    end_time = time.perf_counter()
    logging.info(f"{comment} executed in {end_time - start_time} s")

    return result
