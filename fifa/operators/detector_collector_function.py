import json
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction, ReduceFunction, AggregateFunction


class DetectorCollectorFunction(ProcessWindowFunction):
    def process(self, key: tuple, ctx: ProcessWindowFunction.Context, elements: Iterable[dict]) -> str:
        result = {}
        for element in elements:
            result.update(element)
        yield json.dumps(result)


class DetectorReducerFunction(ReduceFunction):
    def reduce(self, accumulator, value):
        accumulator.update(value)
        return accumulator


class DetectorAggregatorFunction(AggregateFunction):
    def create_accumulator(self):
        return {}

    def add(self, value, accumulator):
        accumulator.update(value)
        return accumulator

    def get_result(self, accumulator):
        return json.dumps(accumulator)

    def merge(self, acc_a, acc_b):
        pass
