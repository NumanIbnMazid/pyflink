import logging
from base64 import b64encode
import json

import cv2


def encode_image(image):
    _, compressed_image = cv2.imencode(".jpg", image)
    return b64encode(compressed_image.tobytes()).decode("ascii")


class FrameGenerator:
    def run(self, value):
        # value = json.loads(value)
        logging.info(f"Getting video from: {value['filename']}")
        print(f"Getting video from: {value['filename']}")
        path = f"videos/videos/{value['filename']}"
        with open(path) as f:
            capture = cv2.VideoCapture(f.name)

            while True:
                if not capture.isOpened():
                    break
                ret, frame = capture.read()

                if ret:
                    frame_index = capture.get(cv2.CAP_PROP_POS_FRAMES)

                    if not (frame_index % 15 < 1):
                        continue

                    logging.info(f"FileVideoStream: Write frame (Index: {frame_index})")
                    print(f"FileVideoStream: Write frame (Index: {frame_index})")

                    result = encode_image(frame)
                    yield {"recording_id": value["recording_id"], "frame_index": frame_index, "frame": result}
                    logging.info(f"FileVideoStream: Write frame (Index: {frame_index}) finished")
                    print(f"FileVideoStream: Write frame (Index: {frame_index}) finished")
                else:
                    logging.info("FileVideoStream: Ended")
                    print("FileVideoStream: Ended")
                    capture.release()
