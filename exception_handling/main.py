import sys
import os
import logging
import json

from pyflink.common import SimpleStringSchema, WatermarkStrategy, RestartStrategies
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
)

from api import ExceptionProducerFunction, MultiValueProducerFunction, DummyWindowFunction, DummyValuePrinterFunction


def main():
    print("Creating Execution Environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.disable_operator_chaining()
    env.set_parallelism(1)
    env.enable_checkpointing(1000)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
        restart_attempts=6,
        delay_between_attempts=5000,
    ))
    # env.set_restart_strategy(RestartStrategies.failure_rate_restart(
    #     failure_rate=3,
    #     failure_interval=30000,
    #     delay_interval=2000,
    # ))

    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-1.16.0.jar')
    env.add_jars("file://{}".format(kafka_jar))

    # consume data from kafka source topic
    print("Setting up Kafka source")
    kafka_consumer = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("test-source-topic") \
        .set_group_id("test_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds_main = env.from_source(kafka_consumer, WatermarkStrategy.no_watermarks(), "kafka-source")

    # setup side output
    # ds_errors = OutputTag("errors", Types.STRING())

    # perform transformation
    print("Setting up operations")
    # topology definition
    ds_result = (
        ds_main
        .process(MultiValueProducerFunction()).name("multi_value_producer_function")
        .key_by(lambda x: 1)
        .count_window(3)
        .process(DummyWindowFunction()).name("window_function")
    )

    ds_api_1 = (
        ds_result
        .key_by(lambda x: 1)
        .process(ExceptionProducerFunction(), output_type=Types.STRING()).name("exception_producer_function")
    )

    ds_api_2 = (
        ds_result
        .key_by(lambda x: 1)
        .process(DummyValuePrinterFunction(), output_type=Types.STRING()).name("dummy_value_printer_function")
    )

    # ds_side = (
    #     ds_api_1
    #     .get_side_output(ds_errors).name("error_stream")
    # )

    # print out the results on the console

    # ds_api_1.print()
    # ds_api_2.print()
    # ds_side.print()

    # produce data to kafka sink topic
    print("Setting up Kafka sink")
    kafka_producer_main = KafkaSink.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("test-main")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # kafka_producer_error = KafkaSink.builder() \
    #     .set_bootstrap_servers("broker:29092") \
    #     .set_record_serializer(
    #     KafkaRecordSerializationSchema.builder()
    #     .set_topic("test-error")
    #     .set_value_serialization_schema(SimpleStringSchema())
    #     .build()
    # ) \
    #     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    #     .build()

    ds_api_1.sink_to(kafka_producer_main).name("main-sink")
    ds_api_2.sink_to(kafka_producer_main).name("main-sink")
    # ds_side.sink_to(kafka_producer_error).name("error-sink")

    # execute
    print("Executing Environment")
    env.execute("exception_handling")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main()
