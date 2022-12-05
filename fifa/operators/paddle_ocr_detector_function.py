from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.paddle_ocr_detector import PaddleOcrDetector


class PaddleOcrDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = PaddleOcrDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
