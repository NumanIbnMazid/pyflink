import sys
import os
import logging
import json

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer, FlinkKafkaProducer,
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
)

from udfs import FrameGeneratorFunction, Fifa2020Function


def main():
    print("Creating Execution Environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    #env.set_parallelism(1)
    env.disable_operator_chaining()

    # add dependencies
    print("Adding dependencies")
    #python_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'udfs')
    #env.add_python_file(file_path=python_dep)
    data_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'videos.zip')
    env.add_python_archive(archive_path=data_dep, target_dir="videos")
    
    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-1.16.0.jar')
    env.add_jars("file://{}".format(kafka_jar))
    
    # consume data from kafka source topic
    print("Setting up Kafka source")
    #deserialization_schema = JsonRowDeserializationSchema.builder() \
    #    .type_info(type_info=Types.ROW_NAMED(["id", "filename"], [Types.INT(), Types.STRING()])).build()

    #kafka_consumer = FlinkKafkaConsumer(
    #    topics='test-source-topic',
    #    deserialization_schema=SimpleStringSchema(),
    #    properties={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})

    #ds = env.add_source(kafka_consumer).name("kafka_test-source-topic")

    kafka_consumer = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("test-source-topic") \
        .set_group_id("test_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(kafka_consumer, WatermarkStrategy.no_watermarks(), "kafka-source")

    kafka_control = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("test-control-topic") \
        .set_group_id("test_control") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds_control = env.from_source(kafka_control, WatermarkStrategy.no_watermarks(), "kafka-control")

    #ds = env.from_collection([
    # source topic
    #    {"id": 1, "filename": "index_720p30_00001.ts"},
    #    {"id": 2, "filename": "index_720p30_00002.ts"},
    # control topic
    #    {"id": 1, "state": "aborted"},
    #    {"id": 2, "state": "aborted"},
    #    {"id": 45, "state": "aborted"},
    #])
    
    # perform transformation
    print("Setting up operations")
    ds = ds.process(FrameGeneratorFunction()).name("frame_generator") \
        .connect(ds_control) \
        .key_by(lambda x: x["id"], lambda x: json.loads(x)["id"]) \
        .process(Fifa2020Function(), output_type=Types.STRING()).name("fifa_detector")

    # produce data to kafka sink topic
    print("Setting up Kafka sink")
    #serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
    #    type_info=Types.ROW_NAMED(["results"], [Types.STRING()])).build()

    #kafka_producer = FlinkKafkaProducer(
    #    topic='test-sink-topic',
    #    serialization_schema=SimpleStringSchema(),
    #    producer_config={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})

    #ds.add_sink(kafka_producer).name("kafka_test-sink-topic")

    kafka_producer = KafkaSink.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("test-sink-topic")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    #ds.sink_to(kafka_producer).name("kafka-sink")

    ds.print()

    # execute
    print("Executing Environment")
    env.execute("video_processing")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main()

