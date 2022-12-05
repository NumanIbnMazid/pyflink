from pyflink.datastream import RuntimeContext, KeyedProcessFunction

from detectors.fifa_detector import FifaDetector


class FifaDetectorFunction(KeyedProcessFunction):
    def __init__(self):
        self.detector = FifaDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            res_dict = result[0].to_dict()
            res_dict.update(recording_id=value["recording_id"], frame_index=value["frame_index"])
            yield res_dict
