from pyflink.datastream import ProcessWindowFunction

from detectors.video_wipe_detector import VideoWipeDetector


class VideoWipeDetectorFunction(ProcessWindowFunction):
    def __init__(self):
        self.detector = VideoWipeDetector()

    def process(self, key, ctx, values):
        if self.detector.name not in values[0]["detectors"]:
            for value in values:
                yield {"recording_id": value["recording_id"], "frame_index": value["frame_index"]}
        else:
            results = self.detector.run(values)
            for result in results:
                yield result
