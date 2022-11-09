import json
import os

from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, CoFlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import ValueStateDescriptor


class Account(CoFlatMapFunction):
    def open(self, ctx: RuntimeContext):
        """Runs only the first time the operator is loaded"""
        amount_descriptor = ValueStateDescriptor("amount", Types.INT())
        status_descriptor = ValueStateDescriptor("status", Types.STRING())
        self.amount_state = ctx.get_state(amount_descriptor)
        self.status_state = ctx.get_state(status_descriptor)

    def flat_map1(self, data):
        """Runs every time new data arrives from the main_ds stream"""
        # start account at $0 for non-existing keys
        data_dict = json.loads(data)
        account_name = data_dict["name"]
        current_amount_state = self.amount_state.value()
        if current_amount_state is None:
            self.amount_state.update(0)

        # check if account is frozen
        current_status_state = self.status_state.value()
        if current_status_state == "inactive":
            yield f"{account_name} -> ${self.amount_state.value()}[inactive], StatusState: {self.amount_state.value()}"

        else:
            # update the total amount
            current_amount = self.amount_state.value()
            amount_to_add = data_dict["add_amount"]
            new_amount = current_amount + amount_to_add

            # update the state
            self.amount_state.update(new_amount)

            yield f"{account_name} -> ${self.amount_state.value()}, AmountState: {self.amount_state.value()}"

    def flat_map2(self, data):
        """Runs every time new data arrives from the status_ds stream"""
        data_dict = json.loads(data)
        account_name = data_dict["name"]
        account_status = data_dict["status"]

        # set account status state
        self.status_state.update(account_status)
        yield f"{account_name} -> AmountState: {self.amount_state.value()}, StatusState: {self.status_state.value()}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'flink-sql-connector-kafka-1.16.0.jar')
    env.add_jars("file://{}".format(kafka_jar))

    # setup kafka sources
    kafka_main = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("main-topic") \
        .set_group_id("test_main") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    main_ds = env.from_source(kafka_main, WatermarkStrategy.no_watermarks(), "kafka-main")

    kafka_status = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_topics("status-topic") \
        .set_group_id("test_status") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    status_ds = env.from_source(kafka_status, WatermarkStrategy.no_watermarks(), "kafka-status")

    # topology definition
    (
        main_ds  # stream with data format {"name": "elon", "add_amount": 5500}
        .connect(status_ds)  # stream with data format {"name": "elon", "status": "inactive"}
        # connecting the 2 streams produces a ConnectedDataStream, but the operator that
        # follows sees the data as coming from 2 separate streams which must be processed using
        # 2 separate functions provided by the "Co" function families such as CoFlatMapFunction above
        .key_by(
            lambda data: json.loads(data)["name"],  # key on which to partition for main_ds
            lambda data: json.loads(data)["name"])  # key on which to partition for status_ds
        .flat_map(Account())  # Account function must be a subclass of "Co" function family
        .print()
    )
    env.execute()


if __name__ == "__main__":
    main()
