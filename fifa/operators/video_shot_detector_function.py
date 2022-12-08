from pyflink.datastream import ProcessWindowFunction

from detectors.video_shot_detector import VideoShotDetector


class VideoShotDetectorFunction(ProcessWindowFunction):
    def __init__(self):
        self.detector = VideoShotDetector()

    def process(self, key, ctx, values):
        if self.detector.name not in values[0]["detectors"]:
            for value in values:
                yield {"recording_id": value["recording_id"], "frame_index": value["frame_index"]}
        else:
            results = self.detector.run(values)
            for result in results:
                yield result
