import logging
from base64 import b64encode
import json
from time import sleep

from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    FlatMapFunction, RuntimeContext, OutputTag,
    KeyedProcessFunction, KeyedCoProcessFunction
)
from pyflink.datastream.state import ValueStateDescriptor

import cv2


def encode_image(image):
    _, compressed_image = cv2.imencode(".jpg", image)
    return b64encode(compressed_image.tobytes()).decode("ascii")


class FrameGeneratorFunction(KeyedProcessFunction):

    def process_element(self, value, ctx: RuntimeContext):
        value = json.loads(value)
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
                    sleep(5)
                    yield {"id": value["id"], "frame": result}
                    logging.info(f"FileVideoStream: Write frame (Index: {frame_index}) finished")
                    print(f"FileVideoStream: Write frame (Index: {frame_index}) finished")
                else:
                    logging.info("FileVideoStream: Ended")
                    print("FileVideoStream: Ended")
                    capture.release()
