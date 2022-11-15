class DummyDetector:
    """A dummy detector"""

    def run(self, value):
        return {"id": value["id"], "frame_index": value["frame_index"], "dummy": True}
