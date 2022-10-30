import logging

from pyflink.datastream import FlatMapFunction


class AggregatorFunction(FlatMapFunction):

    def __init__(self):
        self.result = {}

    def flat_map(self, value):
        if not self.result:
            self.result[value["hash_value"]] = value[0]
        else:
            if self.result.get(value["hash_value"]):
                self.result[value["hash_value"]].update(value[0])
            else:
                self.result[value["hash_value"]] = value[0]

        stored_values = self.result.get(value["hash_value"])
        if "processed" in stored_values and "score_format" in stored_values:
            logging.info(f"Saving results for frame hash: {value["hash_value"]}")
            self.result.pop(value["hash_value"])
