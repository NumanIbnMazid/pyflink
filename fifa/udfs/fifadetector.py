import math
import re
from time import strptime
from typing import Optional, Tuple, Any, List
from base64 import b64decode
import logging
import json

import cv2
import numpy as np
import pytesseract
from pytesseract import Output
from pyflink.datastream import FlatMapFunction

import image_operations
from dto import FIFA2020DTO


def decode_image(content):
    arr = np.frombuffer(b64decode(content), dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


class Fifa2020Function(FlatMapFunction):
    _ADJACENT = "ADJACENT"
    _SEPARATE = "SEPARATE"

    def __init__(self):
        pass

    def flat_map(self, value):
        logging.info("Received encoded frame")
        content = value[1]

        self.image = decode_image(content)

        logging.info("Processing started")
        result = self.process_message()
        logging.info("Processing ended")
        if result:
            yield json.dumps(result[0].to_dict())

    def process_message(self):
        return self.run_detection()

    def detect_ocr(self, image, config, text_pattern, detection_name, detection_type="DICT"):
        logging.debug(f"Start OCR_Detection Execution. Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")
        # logging.info(f"Start OCR_Detection Execution. Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")

        if detection_type == "STRING":
            return pytesseract.image_to_string(image, config=config)

        if detection_type == "DICT":
            d2 = pytesseract.image_to_data(image, config=config, output_type=Output.DICT)

            logging.debug(f"Finished OCR_Detection Execution. Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")
            # logging.info(f"Finished OCR_Detection Execution. Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")

            for text in d2["text"]:
                d_res = re.match(text_pattern, text)
                if d_res is not None:
                    break

            if d_res is not None:
                logging.info(
                    {
                        "message": "FIFA2020_Detector_Result",
                        "OCR_Result_Detected": True,
                        f"{detection_name}_Value": ",".join(d2["text"]),
                        "Detection_Name": detection_name
                    }
                )
                return d_res.groups()
        logging.debug(f"FIFA2020_Detection no results: Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")
        # logging.info(f"FIFA2020_Detection no results: Detection_Name: {detection_name}. Config: {config}. Pattern: {text_pattern}")

    def _prepare_black_element(self, image):
        image = cv2.resize(image, None, fx=2.5, fy=2.5, interpolation=cv2.INTER_CUBIC)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # image = cv2.bitwise_not(image)

        kernel = np.ones((1, 1), np.uint8)
        image = cv2.dilate(image, kernel, iterations=1)
        image = cv2.erode(image, kernel, iterations=1)
        image = cv2.GaussianBlur(image, (5, 5), 0)
        image = cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
        image = cv2.bitwise_not(image)

        return image

    def _prepare_yellow_element(self, image):
        image = cv2.resize(image, None, fx=2.5, fy=2.5, interpolation=cv2.INTER_CUBIC)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        image = cv2.GaussianBlur(image, (5, 5), 0)
        image = cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

        return image

    def _detect_team_name(self, image):
        config = f'--psm 7'
        team_name_pattern = '([A-Za-z- ])+'

        logging.debug(f"Start Team_Name_Detection Execution: {config}")
        # logging.info(f"Start Team_Name_Detection Execution: {config}")

        team_name = self.detect_ocr(image, config, team_name_pattern, "Team_Name_Detector_Result", "STRING")
        team_name = re.sub(r'[^a-zA-Z0-9- ]', '', team_name.strip()).strip()

        logging.debug(f"Finished Team_Name_Detection Execution: {config}")
        # logging.info(f"Finished Team_Name_Detection Execution: {config}")

        if team_name == '' or len(team_name) <= 3:
            return None
        return team_name

    def _detect_game_time(self, image):
        config = f'--psm 11'
        game_time_pattern = '(\d{2,3})\s*([-:])\s*(\d{2})'

        game_time = self.detect_ocr(image, config, game_time_pattern, "Game_Time_Detector_Result")

        if game_time is None:
            return None

        prepared_game_time = self.prepare_time_result(game_time)

        return prepared_game_time

    def _detect_score(self, image) -> Optional[Tuple[int, int]]:
        try:
            total_score = self.detect_ocr(image, "--psm 11", "(\d{1,})\s*([-:])\s*(\d{1,})", "Score_Detector_Result")

            if total_score is None:
                return None

            home_score = total_score[0]
            away_score = total_score[2]

            logging.info(f"_detect_score_adjacent: Home: {home_score}, Away: {away_score}, Total: {total_score}")

            return int(home_score), int(away_score)
        except ValueError:  # no integers detected, expected issue
            return None
        except Exception as e:
            self.log.exception(e)
            return None

    def find_image_elements_by_hsv(self,
                      img,
                      lower_bound,
                      upper_bound,
                      kernel=np.ones((7, 7), np.uint8),
                      threshold_width=30,
                      threhold_height=20
                      ):
        # convert to hsv colorspace
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

        # find the colors within the boundaries
        mask = cv2.inRange(hsv, lower_bound, upper_bound)

        # Remove unnecessary noise from mask
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)

        # Segment only the detected region
        # segmented_img = cv2.bitwise_and(img, img, mask=mask)

        # Find contours from the mask
        contours, hierarchy = cv2.findContours(mask.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        (contours, boundingBoxes) = image_operations.sort_contours(contours, method="left-to-right")

        image_elements = []

        for c in contours:
            # Returns the location and width,height for every contour
            x, y, w, h = cv2.boundingRect(c)

            if w >= threshold_width and h >= threhold_height and w <= 200:
                image_element = img[y:y + h, x:x + w]
                image_elements.append(image_element)

        return image_elements

    def find_object_in_image(self, elements: List, ocr_func):
        # find score
        found_obj = None
        found_element = None
        for idx, element in enumerate(elements):
            found_obj = ocr_func(element)
            if found_obj is not None:
                found_element = idx
                break

        if found_obj is not None:
            del elements[found_element]
        return found_obj

    def prepare_time_result(self, d):
        if d is not None:
            # strptime does not support carry-over times like 72 minutes
            # so we need to the minute/hour calculation ourselves

            # Example 119 minutes
            minutes = int(d[0])
            separator = d[1]

            d_hours = math.floor(minutes / 60)  # floor(119/60) => floor(1,9833) => 1
            d_minutes = minutes % 60  # 119 % 60 = 59
            d_seconds = int(d[2])

            # create string: HH:MM:SS - required to create timestamp & total seconds
            d_game_time = f"{d_hours}{separator}{d_minutes}{separator}{d_seconds}"
            d_timestamp = strptime(d_game_time, f"%H{separator}%M{separator}%S")
            d_seconds_total = d_timestamp.tm_hour * 3600 + d_timestamp.tm_min * 60 + d_timestamp.tm_sec

            # create formatted string: MM:SS - human friendly output
            d_game_time_formatted = ''.join(d)

            return d_game_time_formatted, d_timestamp, d_seconds_total
        return None

    def run_detection(self):
        results = []
        try:
            image_width = image_operations.Rectangle.get_image_width(self.image)
            image_height = image_operations.Rectangle.get_image_height(self.image)

            # operate on top left corner: 25% height, 50% width
            rec = image_operations.Rectangle(0, image_width*0.5, 0, image_height*0.25)

            image = image_operations.Image.crop(self.image, rec)

            # black elements like score & game_time
            lower_bound = np.array([0, 0, 0])
            upper_bound = np.array([180, 255, 41])
            black_elements = self.find_image_elements_by_hsv(image, lower_bound=lower_bound, upper_bound=upper_bound)

            # team names - yellow boxes
            lower_bound = np.array([15, 30, 64])
            upper_bound = np.array([29, 171, 218])
            yellow_elements = self.find_image_elements_by_hsv(image, lower_bound=lower_bound, upper_bound=upper_bound)

            for idx, element in enumerate(black_elements):
                black_elements[idx] = self._prepare_black_element(element)

            for idx, element in enumerate(yellow_elements):
                yellow_elements[idx] = self._prepare_yellow_element(element)

            # find game_time
            game_time = self.find_object_in_image(black_elements, self._detect_game_time)
            logging.info(
                {
                    "message": f"Detected game_time {game_time}",
                    "game_time_detected": game_time is not None,
                    "game_time": game_time,
                }
            )

            # find score
            score = self.find_object_in_image(black_elements, self._detect_score)
            logging.info(
                {
                    "message": f"Detected score {score}",
                    "score_detected": score is not None,
                    "score": score,
                }
            )

            # find home_team
            home_team = self.find_object_in_image(yellow_elements, self._detect_team_name)

            # find away_team
            away_team = self.find_object_in_image(yellow_elements, self._detect_team_name)

            # stream_pos = self.create_stream_pos_message(RecordingDataType.SCORE_POS)
            # if stream_pos is not None:
            #     results.append(stream_pos)
            #
            # stream_pos = self.create_stream_pos_message(RecordingDataType.GAME_TIME_POS)
            # if stream_pos is not None:
            #     results.append(stream_pos)

            if score is not None and game_time is not None and home_team is not None and away_team is not None:
                fifa2020_data = FIFA2020DTO(
                    home_score=score[0],
                    away_score=score[1],
                    score_format='{home} - {away} ({home_team} - {away_team}) ({match_count})',
                    home_team=home_team,
                    away_team=away_team,
                    game_time=game_time[0],
                    game_time_format="%M:%S",  # self.game_time_detector_settings.game_time_format,
                    game_time_seconds=game_time[2]
                )

                # results.append(self.create_detector_data_object("FIFA2020", fifa2020_data))
                results.append(fifa2020_data)
        except Exception as e:
            logging.info(f"Exception: {e}")

        if len(results) == 0:
            logging.debug(f"Nothing to send. Next message...")
            # logging.info(f"Nothing to send. Next message...")

        # logging.info(f"Results: {results}")

        return results
