import json
from typing import Iterable, cast

from pyflink.common import Types, Time
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, WindowFunction, AggregateFunction
from pyflink.datastream.state import ReducingStateDescriptor, ReducingState
from pyflink.datastream.window import (
    CountWindow, CountTumblingWindowAssigner, Trigger,
    TriggerResult, GlobalWindow, TimeWindow, TumblingEventTimeWindows
)


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["event_time"])


class AggregateResultsFunction(AggregateFunction):
    def create_accumulator(self):
        return {}

    def add(self, value, accumulator):
        print("Adding value: ", value, " Current accumulator: ", accumulator)
        accumulator.update(value)
        return accumulator

    def get_result(self, accumulator):
        print("Getting results: ", accumulator)
        return json.dumps(accumulator)

    def merge(self, acc_a, acc_b):
        print("Merging accumulators")
        return acc_a


class CountAndEventTimeTrigger(Trigger):
    def __init__(self, window_size: int):
        self._window_size = window_size
        self._count_state_descriptor = ReducingStateDescriptor("count", lambda a, b: a + b, Types.LONG())

    def on_element(self, element, timestamp: int, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        count_state = cast(ReducingState, ctx.get_partitioned_state(self._count_state_descriptor))
        count_state.add(1)
        if count_state.get() >= self._window_size or window.max_timestamp() <= ctx.get_current_watermark():
            return TriggerResult.FIRE_AND_PURGE
        else:
            ctx.register_event_time_timer(window.max_timestamp())
            return TriggerResult.CONTINUE

    def on_processing_time(self, time: int, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        return TriggerResult.CONTINUE

    def on_event_time(self, time: int, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        if time == window.max_timestamp():
            return TriggerResult.FIRE_AND_PURGE
        else:
            return TriggerResult.CONTINUE

    def on_merge(self, window: TimeWindow, ctx: 'Trigger.OnMergeContext') -> None:
        pass

    def clear(self, window: TimeWindow, ctx: 'Trigger.TriggerContext') -> None:
        ctx.delete_event_time_timer(window.max_timestamp())
        count_state = ctx.get_partitioned_state(self._count_state_descriptor)
        count_state.clear()


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    # data_stream = env.from_collection([
    #     (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
    #     type_info=Types.TUPLE([Types.INT(), Types.STRING()]))

    data_stream = env.from_collection([
        {"name": "sharif", "event_time": 1, "eat": True},
        {"name": "sunny", "event_time": 1, "eat": True},
        {"name": "sunny", "event_time": 2, "work": True},
        {"name": "sharif", "event_time": 2, "sleep": True},
        {"name": "sharif", "event_time": 3, "work": True},
        {"name": "sharif", "event_time": 4, "walk": True},
        {"name": "sharif", "event_time": 4, "talk": True},
        {"name": "numan", "event_time": 4, "eat": True},
        {"name": "sunny", "event_time": 4, "sleep": True},
        {"name": "numan", "event_time": 5, "sleep": True},
        {"name": "numan", "event_time": 6, "work": True},
        {"name": "numan", "event_time": 7, "walk": True},
        {"name": "hasan", "event_time": 7, "eat": True},
        ],
    )

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())

    ds = (
        data_stream
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x["name"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(3)))
        .trigger(CountAndEventTimeTrigger(3))
        .aggregate(AggregateResultsFunction(), output_type=Types.STRING())
    )

    # define the sink
    # ds.print()

    # submit for execution
    env.execute()
