from typing import Iterable

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    data_stream = env.from_collection([
        ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 5), ('hi', 8), ('hi', 9), ('hi', 15)],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

    # define the watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2))) \
        .process(CountWindowProcessFunction(),
                 Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))

    # define the sink
    ds.print()

    # submit for execution
    env.execute()
