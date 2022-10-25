from time import sleep
import json
import logging


def add_count(data):
    # parse the json
    json_data = json.loads(data[1])
    json_data['count'] += 1
    logging.info(f"Processing record {data[0]} for 2 seconds...")
    print(f"Processing record {data[0]} for 2 seconds...")
    sleep(2)
    return data[0], json_data


def filter_by_country(data):
    # the json data could be accessed directly, no need to parse it again using json.loads
    return "China" in data[1]['addr']['country']


def verify(data):
    data[1].update(verified=True)
    return data


def modify(data):
    data[1].update(modified=True)
    return data
