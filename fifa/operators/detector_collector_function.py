import json

from pyflink.common import Types
from pyflink.datastream import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor


class DetectorCollectorFunction(KeyedProcessFunction):
    def __init__(self, merge_ops):
        self.merge_ops = merge_ops
        self.detector_results = None

    def open(self, ctx: RuntimeContext):
        descriptor = ValueStateDescriptor("results", Types.PICKLED_BYTE_ARRAY())
        self.detector_results = ctx.get_state(descriptor)

    def process_element(self, value, ctx: RuntimeContext):
        if self.detector_results.value() is None:
            self.detector_results.update([])
        current_value = self.detector_results.value()
        if len(current_value) <= self.merge_ops - 2:
            current_value.append(value)
            self.detector_results.update(current_value)
            return
        if len(current_value) == self.merge_ops - 1:
            current_value.append(value)
            self.detector_results.update(current_value)
            final_result = {}
            for result in self.detector_results.value():
                final_result.update(result)
            yield json.dumps(final_result)
