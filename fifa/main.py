import sys
import os
import logging

from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer

from framegenerator import FrameGeneratorFunction
from fifadetector import Fifa2020Function


def main():
    print("Creating Execution Environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    #env.set_parallelism(1)
    env.disable_operator_chaining()

    # add dependencies
    print("Adding dependencies")
    python_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'udfs')
    env.add_python_file(file_path=python_dep)
    data_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'videos.zip')
    env.add_python_archive(archive_path=data_dep, target_dir="videos")
    
    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka_2.11-1.14.4.jar')
    env.add_jars("file://{}".format(kafka_jar))
    
    # consume data from kafka source topic
    print("Setting up Kafka source")
    #deserialization_schema = JsonRowDeserializationSchema.builder() \
    #    .type_info(type_info=Types.ROW_NAMED(["id", "filename"], [Types.INT(), Types.STRING()])).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='test-source-topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})

    ds = env.add_source(kafka_consumer).name("kafka_test-source-topic")

    #ds = env.from_collection([
    #    {"id": 1, "filename": "index_720p30_00001.ts"},
    #    {"id": 2, "filename": "index_720p30_00002.ts"},
    #])
    
    # perform transformation
    print("Setting up operations")
    ds = ds.flat_map(FrameGeneratorFunction()).name("frame_generator") \
           .flat_map(Fifa2020Function(), output_type=Types.STRING()).name("fifa_detector") \
           .start_new_chain().set_parallelism(2)
    
    # produce data to kafka sink topic
    print("Setting up Kafka sink")
    #serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
    #    type_info=Types.ROW_NAMED(["results"], [Types.STRING()])).build()

    kafka_producer = FlinkKafkaProducer(
        topic='test-sink-topic',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})

    ds.add_sink(kafka_producer).name("kafka_test-sink-topic")

    #ds.print()

    # execute
    print("Executing Environment")
    env.execute("video_processing")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main()

