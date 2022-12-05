from pyflink.datastream import RuntimeContext, ProcessFunction

from detectors.video_wipe_detector import VideoWipeDetector


class VideoWipeDetectorFunction(ProcessFunction):
    def __init__(self):
        self.detector = VideoWipeDetector()

    def process_element(self, value, ctx: RuntimeContext):
        result = self.detector.run(value)
        if result:
            yield result
