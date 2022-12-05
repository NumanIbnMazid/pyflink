class PaddleOcrDetector:
    """A dummy paddle ocr detector"""

    def __init__(self):
        self.name = "paddle_ocr"

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "paddle_ocr": True}
