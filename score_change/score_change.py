import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy


detector_data = [
    "230000 03:50 0-0",
    "240000 04:00 0-0",
    "250000 04:10 0-0",
    "260000 04:20 0-0",
    "270000 04:30 0-1",
    "280000 04:40 0-1",
    "290000 04:50 0-1",
    "300000 05:00 0-1",
    "310000 05:10 0-1",
    "320000 05:20 0-1",
    "330000 05:30 0-1",
    "340000 05:40 0-1",
    "350000 05:50 0-1",
    "360000 06:00 1-1",
    "370000 06:10 1-1",
    "380000 06:20 1-1",
    "390000 06:30 1-1",
    "400000 06:40 1-1",
]


def get_score_change(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    if input_path is not None:
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing score change example with default input data set.")
        print("Use --input to specify file input.")
        ds = env.from_collection(detector_data)

    def split(line):
        # Pass the whole line as an iterable to map func
        yield from [line]

    def map_func(i):
        # Split by default space
        splitted_line = i.split()
        # return tuple which would look like: ('0-0', ('230000', '03:50')) : (score, (stream_pos, game_time))
        return (splitted_line[-1], (splitted_line[0], splitted_line[1]))

    def reduce_func(i, j):
        # TODO: remove redundant check (It should always match as it is already grouped)
        if i[0] == j[0]:
            # TODO: replace this block of code with one liner min() func
            # Get the score according to the lowest stream position
            if i[-1][0] <= j[-1][0]:
                result = i
            else:
                result = j
            return result
        # TODO: Remove redundant else block
        else:
            # TODO: Handle fallback if needed
            print("Not matching condition!")

    # compute score change
    # Key by score (Group similar scores together)
    ds = ds.flat_map(split) \
        .map(map_func) \
        .key_by(lambda i: i[0]) \
        .reduce(reduce_func)

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    get_score_change(known_args.input, known_args.output)