import sys
import logging
from time import sleep

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction


class Detector:
    def __init__(self):
        print("Initializing detector...")
        sleep(3)
        print("Detector initialization complete!")

    def process(self, value):
        print("Processing value: ", value)
        return value * 2


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        print("Initializing resources...")
        self.detector = Detector()
        print("Initialization complete!")

    def map(self, value):
        return self.detector.process(value)


def object_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 2. create source DataStream
    seq_num_source = NumberSequenceSource(1, 20)
    ds = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    # 3. define the execution logic
    ds = ds.map(MyMapFunction(), output_type=Types.INT()).name("double_value")

    # 4. create sink and emit result to sink
    #output_path = '/tmp/output/'
    #file_sink = FileSink \
    #    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    #    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    #    .build()
    #ds.sink_to(file_sink)
    ds.print()

    # 5. execute the job
    env.execute('Object Demo')


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    object_demo()

