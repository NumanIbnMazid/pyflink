from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.sleep_detector import SleepDetector


class SleepDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = SleepDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
