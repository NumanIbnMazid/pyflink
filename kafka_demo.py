import sys
import os
import logging

from pyflink.common import Row
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


def kafka_demo():
    print("Creating Execution Environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka_2.11-1.14.4.jar')
    env.add_jars("file://{}".format(kafka_jar))
    
    # consume data from kafka source topic
    print("Setting up Kafka source")
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW_NAMED(["name", "age"], [Types.STRING(), Types.INT()])).build()
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='test-source-topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})
    
    ds = env.add_source(kafka_consumer)
    
    # perform transformation
    print("Setting up operations")
    def transform(data):
        print(f"Received: {data}")
        result = Row(data.name.upper(), data.age + 1)
        print(f"Result: {result}")
        return result
    
    ds = ds.map(transform, output_type=Types.ROW([Types.STRING(), Types.INT()]))
    
    # produce data to kafka sink topic
    print("Setting up Kafka sink")
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW_NAMED(["name", "age"], [Types.STRING(), Types.INT()])).build()
    
    kafka_producer = FlinkKafkaProducer(
        topic='test-sink-topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'broker:29092', 'group.id': 'test_group'})
    
    ds.add_sink(kafka_producer)

    # execute
    print("Executing Environment")
    env.execute("kafka_demo")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    kafka_demo()

