import random

from pyflink.common import Types
from pyflink.datastream import OutputTag, RuntimeContext, KeyedProcessFunction, ProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
import requests

ds_errors = OutputTag("errors", Types.STRING())


class ExceptionProducerFunction(KeyedProcessFunction):
    def __init__(self):
        self.count = None

    def open(self, ctx: RuntimeContext):
        descriptor = ValueStateDescriptor("count", Types.INT())
        self.count = ctx.get_state(descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_count = self.count.value()
        if not current_count:
            current_count = 0
        current_count += 1
        self.count.update(current_count)
        print("Exception count state: ", self.count.value())

        url = "http://httpbin.org/delay/2" if value == "timeout" else "http://httpbin.org/status/200,201,202"
        timeout = random.randint(1, 4) if value == "timeout" else None
        try:
            print(f"Requesting with timeout value {timeout}")
            res = requests.get(url, timeout=timeout)
            res.raise_for_status()
            yield f"Main Stream: {res.status_code} State: {self.count.value()}"
        # except requests.exceptions.HTTPError:
        #     yield ds_errors, f"Error Stream: {res.reason}"
        except requests.exceptions.Timeout:
            raise


class DummyWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        for i in elements:
            yield i


class MultiValueProducerFunction(KeyedProcessFunction):
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if value == "pass":
            for i in range(1, 7):
                yield i
        else:
            for i in range(1, 7):
                if i % 3 == 0:
                    yield "timeout"
                else:
                    yield i


class DummyValuePrinterFunction(KeyedProcessFunction):
    def __init__(self):
        self.count = None

    def open(self, ctx: RuntimeContext):
        descriptor = ValueStateDescriptor("dummy_count", Types.INT())
        self.count = ctx.get_state(descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_count = self.count.value()
        if not current_count:
            current_count = 0
        current_count += 1
        self.count.update(current_count)
        print("Dummy count state: ", self.count.value())

        yield f"Dummy Value Printer: {value} State: {self.count.value()}"
