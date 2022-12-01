import json
import logging
import os
import sys

from pyflink.common import Types, WatermarkStrategy, Time, SimpleStringSchema, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import EventTimeSessionWindows


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        value = json.loads(value)
        return int(value["timestamp"])


class UserSessionFunction(AggregateFunction):
    def create_accumulator(self):
        return {}

    def add(self, value, user_activity):
        user_activity["user"] = value["user"]
        user_activity["activity_start"] = min(value["timestamp"], user_activity.get("activity_start", 1000000))
        user_activity["activity_end"] = max(value["timestamp"], user_activity.get("activity_end", -1000000))
        user_activity["num_interactions"] = user_activity.get("num_interactions", 0) + 1
        return user_activity

    def get_result(self, user_activity):
        print(user_activity)
        return json.dumps(user_activity)

    def merge(self, acc_a, acc_b):
        print("merging")
        pass


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-1.16.0.jar')
    env.add_jars("file://{}".format(kafka_jar))

    # consume data from kafka source topic
    print("Setting up Kafka source")

    kafka_consumer = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("user-session") \
        .set_group_id("test_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # define the watermark strategy
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(MyTimestampAssigner())

    data_stream = env.from_source(kafka_consumer,
                                  WatermarkStrategy.no_watermarks(),
                                  "kafka-source")

    # define the source
    # data_stream = env.from_collection([
    #     {"id": 1, "user": "sharif", "timestamp": 1000},
    #     {"id": 2, "user": "numan", "timestamp": 1000},
    #     {"id": 3, "user": "hasan", "timestamp": 2000},
    #     {"id": 4, "user": "numan", "timestamp": 2000},
    #     {"id": 5, "user": "sunny", "timestamp": 3000},
    #     {"id": 6, "user": "sunny", "timestamp": 4000},
    #     {"id": 7, "user": "numan", "timestamp": 4000},
    #     {"id": 8, "user": "sunny", "timestamp": 6000},
    #     {"id": 9, "user": "sunny", "timestamp": 9000},
    #     {"id": 10, "user": "numan", "timestamp": 9000},
    # ])

    # define the topology
    ds = (
        data_stream
        .assign_timestamps_and_watermarks(watermark_strategy).name("timestamps_and_watermarks")
        .map(lambda x: json.loads(x)).name("json_deserializer")
        .key_by(lambda x: x["user"], key_type=Types.STRING())
        .window(EventTimeSessionWindows.with_gap(Time.seconds(3)))
        .aggregate(UserSessionFunction())
    )

    # define the sink
    # ds.print()

    # submit for execution
    env.execute()
