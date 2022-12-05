class GameTimeDetector:
    """A dummy game time detector"""

    def __init__(self):
        self.name = "game_time"

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "game_time": True}
