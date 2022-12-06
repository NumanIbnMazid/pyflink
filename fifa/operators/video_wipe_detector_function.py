from pyflink.datastream import RuntimeContext, KeyedProcessFunction

from detectors.video_wipe_detector import VideoWipeDetector


class VideoWipeDetectorFunction(KeyedProcessFunction):
    def __init__(self):
        self.detector = VideoWipeDetector()

    def process_element(self, value, ctx: RuntimeContext):
        if self.detector.name not in value["detectors"]:
            yield {"recording_id": value["recording_id"], "frame_index": value["frame_index"]}
        else:
            result = self.detector.run(value)
            if result:
                yield result
