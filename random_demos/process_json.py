import os
import copy
import logging
import sys
import json

from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, CoMapFunction
from pyflink.datastream.functions import RuntimeContext
#from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)

from json_processors import add_count, verify, modify


class JoinResultFunction(CoMapFunction):
    def __init__(self):
        self.state = {}

    #def open(self, runtime_context: RuntimeContext):
    #    self.state = runtime_context.get_state(ValueStateDescriptor(
    #        "my_state", Types.PICKLED_BYTE_ARRAY()))

    def map1(self, value):
        result = None
        if value[0] not in self.state:
            self.state.update({value[0]: value[1]})
        else:
            res = copy.deepcopy(value[1])
            res.update(self.state[value[0]])
            result = (value[0], json.dumps(res))
            self.state.pop(value[0])
        return result

    def map2(self, value):
        result = None
        if value[0] not in self.state:
            self.state.update({value[0]: value[1]})
        else:
            res = copy.deepcopy(value[1])
            res.update(self.state[value[0]])
            result = (value[0], json.dumps(res))
            self.state.pop(value[0])
        return result
    

def process_json_data():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.disable_operator_chaining()

    # add python dependencies
    dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'my_udfs')
    env.add_python_file(dep)

    # define the source
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "count": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "count": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "count": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "count": 32, "addr": {"country": "China", "city": "Hangzhou"}}')]
    ).name("JSON Data Source")

    # define operations
    ds = ds.map(add_count).name("add_1_to_count")
    verified_ds = ds.map(verify).name("add_verify")
    modified_ds = ds.map(modify).name("add_modify")
    final_ds = verified_ds.connect(modified_ds).map(
        JoinResultFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING()])).name("join_data")

    # create sink and emit result to sink
    output_path = '/tmp/output/'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()
    final_ds.sink_to(file_sink).name("File Sink")
    #final_ds.print()

    # submit for execution
    env.execute("Process JSON Data")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    process_json_data()
