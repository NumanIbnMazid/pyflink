class SleepDetector:
    """A dummy sleep detector"""

    def run(self, value):
        return {"id": value["id"], "frame_index": value["frame_index"], "sleep": True}
