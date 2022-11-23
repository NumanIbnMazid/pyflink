from dataclasses import dataclass

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, AggregateFunction
from pyflink.datastream.window import EventTimeSessionWindows


@dataclass
class UserActivity:
    user: str = None
    activity_start: int = 1000000
    activity_end: int = -1000000
    num_interactions: int = 0


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["timestamp"])


class UserSessionFunction(AggregateFunction):
    def create_accumulator(self):
        return UserActivity()

    def add(self, value, user_activity: UserActivity):
        user_activity.user = value["user"]
        user_activity.activity_start = min(value["timestamp"], user_activity.activity_start)
        user_activity.activity_end = max(value["timestamp"], user_activity.activity_end)
        user_activity.num_interactions += 1
        return user_activity

    def get_result(self, user_activity: UserActivity):
        return user_activity

    def merge(self, acc_a, acc_b):
        pass


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # define the source
    data_stream = env.from_collection([
        {"id": 1, "user": "sharif", "timestamp": 1},
        {"id": 2, "user": "numan", "timestamp": 1},
        {"id": 3, "user": "hasan", "timestamp": 2},
        {"id": 4, "user": "numan", "timestamp": 6},
        {"id": 5, "user": "sunny", "timestamp": 3},
        {"id": 6, "user": "sunny", "timestamp": 4},
        {"id": 7, "user": "numan", "timestamp": 5},
        {"id": 8, "user": "sunny", "timestamp": 7},
        {"id": 9, "user": "sunny", "timestamp": 6},
    ])

    # define the watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # define the topology
    ds = (
        data_stream
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x["user"], key_type=Types.STRING())
        .window(EventTimeSessionWindows.with_gap(Time.milliseconds(3)))
        .aggregate(UserSessionFunction())
    )

    # define the sink
    ds.print()

    # submit for execution
    env.execute()
