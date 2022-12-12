from time import sleep
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)

word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "The ++heartache++, and the thousand natural shocks",
                   "Be all my sins remember'd."]


#def movie_transform(input_path, output_path1, output_path2):
def movie_transform(out1, out2):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.disable_operator_chaining()
    # write all the data to one file
    env.set_parallelism(1)
    #env.set_parallelism(2)
    print("inside word_count")
    # define the source
    
#    ds = env.from_source(
#        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
#                                                    input_path)
#                            .process_static_file_set().build(),
#        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
#        source_name="file_source"
#    )
    ds = env.from_collection(word_count_data)
    

    def split(line):
        logging.info("Sleeping for 7 seconds...")
        print("Sleeping for 7 seconds...")
        sleep(7)
        yield from line.split("\n")

    def sleeper(line):
        logging.info("Sleeping for 5 seconds...")
        print("Sleeping for 5 seconds...")
        sleep(5)
        yield from line.split("\n")

    # compute word count
    res1 = ds.flat_map(split) \
        .map(lambda i: i.replace("-","+"), output_type=Types.STRING())
        # .key_by(lambda i: i[0]) \
        # .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # compute with sleep
    res2 = ds.flat_map(sleeper) \
        .map(lambda i: i.replace("+","-"), output_type=Types.STRING())

    print("sinking output")
    # define the sink
        
    res1.sink_to(
        sink=FileSink.for_row_format(
            base_path=out1,
            encoder=Encoder.simple_string_encoder())
        .with_output_file_config(
            OutputFileConfig.builder()
            .build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    res2.sink_to(
        sink=FileSink.for_row_format(
            base_path=out2,
            encoder=Encoder.simple_string_encoder())
        .with_output_file_config(
            OutputFileConfig.builder()
            .build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    res1.print()
    res2.print()

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
        '--output1',
        dest='output1',
        required=False,
        help='Output1 file to write results to.')
    parser.add_argument(
        '--output2',
        dest='output2',
        required=False,
        help='Output2 file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    #movie_transform(known_args.input, known_args.output1, known_args.output2)
    movie_transform(known_args.output1, known_args.output2)
