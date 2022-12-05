class VideoShotDetector:
    """A dummy video shot detector"""

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "video_shot": True}
