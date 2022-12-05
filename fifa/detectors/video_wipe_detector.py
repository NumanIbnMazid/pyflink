class VideoWipeDetector:
    """A dummy video wipe detector"""

    def __init__(self):
        self.name = "video_wipe"

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "video_wipe": True}
