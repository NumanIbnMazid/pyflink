from pyflink.datastream import RuntimeContext, KeyedProcessFunction

from detectors.video_shot_detector import VideoShotDetector


class VideoShotDetectorFunction(KeyedProcessFunction):
    def __init__(self):
        self.detector = VideoShotDetector()

    def process_element(self, value, ctx: RuntimeContext):
        if self.detector.name not in value["detectors"]:
            yield {"recording_id": value["recording_id"], "frame_index": value["frame_index"]}
        else:
            result = self.detector.run(value)
            if result:
                yield result
