from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.game_time_detector import GameTimeDetector


class GameTimeDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = GameTimeDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
