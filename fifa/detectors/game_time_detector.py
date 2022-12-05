class GameTimeDetector:
    """A dummy game time detector"""

    def run(self, value):
        return {"recording_id": value["recording_id"], "frame_index": value["frame_index"], "game_time": True}
