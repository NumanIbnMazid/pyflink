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
    timestamps = defaultdict(lambda: 1)
    for i in range(1, 11):
        # create monotonous timestamps per user
        user = random.choice(['sharif', 'hasan', 'numan', 'sunny'])
        current_timestamp = timestamps[user]
        timestamps[user] = random.randint(current_timestamp, current_timestamp + 5)

        # produce data to the kafka topic
        data = {
            'id': i,
            'user': user,
            'timestamp': timestamps[user]
        }
        msg = json.dumps(data)
        producer.poll(0)
        producer.produce('user-session', msg.encode('utf-8'), callback=receipt)
        producer.flush()
        
        # random delay between 0 and 3 seconds
        time.sleep(random.randint(0, 3))


if __name__ == '__main__':
    main()
