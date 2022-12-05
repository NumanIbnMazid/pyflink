from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.video_shot_detector import VideoShotDetector


class VideoShotDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = VideoShotDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
