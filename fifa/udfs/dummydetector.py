import logging

from pyflink.datastream import FlatMapFunction


class DummyFunction(FlatMapFunction):
    def __init__(self):
        pass

    def flat_map(self, value):
        logging.info("Received frame hash")
        result = {"processed": True}
        logging.info("Emitting results")
        yield result
