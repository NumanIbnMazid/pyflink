from pyflink.datastream import RuntimeContext, KeyedProcessFunction

from detectors.frame_generator import FrameGenerator


class FrameGeneratorFunction(KeyedProcessFunction):
    def __init__(self):
        self.frame_generator = FrameGenerator()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.frame_generator.run(value)
        yield from result
