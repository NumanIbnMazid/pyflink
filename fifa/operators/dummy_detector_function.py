from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.dummy_detector import DummyDetector


class DummyDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = DummyDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
