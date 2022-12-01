import json
import logging
import random
import sys
import time
from collections import defaultdict

from confluent_kafka import Producer

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

producer = Producer({'bootstrap.servers': 'localhost:9092'})
logger.info('Kafka Producer has been initiated...')


def receipt(err, msg):
    if err is not None:
        logger.error('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)


def main():
    # timestamps = defaultdict(lambda: 1)
    # for i in range(1, 11):
    #     # create monotonous timestamps per user
    #     # user = random.choice(['sharif', 'hasan', 'numan', 'sunny'])
    #     user = random.choice(['sharif'])
    #     current_timestamp = timestamps[user]
    #     timestamps[user] = random.randint(current_timestamp, current_timestamp + 5)
    #
    #     # produce data to the kafka topic
    #     data = {
    #         'id': i,
    #         'user': user,
    #         'timestamp': timestamps[user] * 1000
    #     }
    #     msg = json.dumps(data)
    #     producer.poll(0)
    #     producer.produce('user-session', msg.encode('utf-8'), callback=receipt)
    #     producer.flush()
    #
    #     # wait before sending next message
    #     time.sleep(5)

    dummy_data = [
        {"id": 1, "user": "sharif", "timestamp": 1000},
        {"id": 2, "user": "sharif", "timestamp": 1000},
        {"id": 3, "user": "sharif", "timestamp": 2000},
        {"id": 4, "user": "sharif", "timestamp": 2000},
        {"id": 5, "user": "sharif", "timestamp": 3000},
        {"id": 6, "user": "sharif", "timestamp": 4000},
        {"id": 7, "user": "sharif", "timestamp": 4000},
        {"id": 8, "user": "sharif", "timestamp": 6000},
        {"id": 9, "user": "sharif", "timestamp": 16000},
        {"id": 10, "user": "sharif", "timestamp": 16000},
    ]

    for d in dummy_data:
        msg = json.dumps(d)
        producer.poll(0)
        producer.produce('user-session', msg.encode('utf-8'), callback=receipt)
        producer.flush()

        time.sleep(5)


if __name__ == '__main__':
    main()
