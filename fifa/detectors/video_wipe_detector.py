class VideoWipeDetector:
    """A dummy video wipe detector"""

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "video_wipe": True}
