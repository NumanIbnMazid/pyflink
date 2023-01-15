import argparse
import logging
import sys

from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf
import os
import time


def word_count(output_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")

    # define the source
    print("Executing word_count example with user input data set.")
    # print("*** Data path :- ", os.path.join(os.getcwd(), "data"))

    # NOTE: `path`: required: path to a directory
    # NOTE: `source.monitor-interval`: continuous directory watching by configuring the option
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .build())
        .option(
            'path', os.path.join(os.getcwd(), "input"),
        )
        .format('csv')
        .build())
    tab = t_env.from_path('source')

    # define the sink
    if output_path is not None:
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .option('path', os.path.join(os.getcwd(), "output"))
                .format(FormatDescriptor.for_format('canal-json')
                        .build())
                .build())
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('print')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .build())

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row):
        for s in line[0].split():
            # time.sleep(2)
            yield Row(s)

    # compute word count
    tab.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count) \
        .execute_insert('sink') \
        .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


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

    word_count(known_args.output)
