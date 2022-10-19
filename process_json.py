from time import sleep
import json
import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment


def process_json_data():
    env = StreamExecutionEnvironment.get_execution_environment()

    # define the source
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')]
    )

    def add_tel(data):
        # parse the json
        json_data = json.loads(data[1])
        json_data['tel'] += 1
        logging.info("Going to sleep for 5 seconds...")
        sleep(5)
        return data[0], json_data

    def sub_tel(data):
        data[1]['tel'] -= 2
        return data

    def filter_by_country(data):
        # the json data could be accessed directly, there is no need to parse it again using
        # json.loads
        return "China" in data[1]['addr']['country']

    ds.map(add_tel).filter(filter_by_country).start_new_chain().map(sub_tel).print()

    # submit for execution
    env.execute_async("Processing Job")

def reprocess_json_data():
    env = StreamExecutionEnvironment.get_execution_environment()

    # define the source
    ds = env.from_collection(
        collection=[
            (5, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (6, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (7, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (8, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')]
    )

    def add_tel(data):
        # parse the json
        json_data = json.loads(data[1])
        json_data['tel'] += 1
        logging.info("Going to sleep for 4 seconds...")
        sleep(4)
        return data[0], json_data

    def filter_by_country(data):
        # the json data could be accessed directly, there is no need to parse it again using
        # json.loads
        return "Germany" in data[1]['addr']['country']

    ds.map(add_tel).filter(filter_by_country).print()

    # submit for execution
    env.execute_async("Reprocessing Job")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    process_json_data()
    reprocess_json_data()
