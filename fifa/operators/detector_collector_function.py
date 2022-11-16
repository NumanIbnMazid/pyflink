import json
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction


class DetectorCollectorFunction(ProcessWindowFunction):
    def process(self, key: tuple, ctx: ProcessWindowFunction.Context, elements: Iterable[dict]) -> str:
        result = {}
        for element in elements:
            result.update(element)
        yield json.dumps(result)
