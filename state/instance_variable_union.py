import json
import os

from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


class Account(FlatMapFunction):
    def open(self, ctx: RuntimeContext):
        """Runs only the first time the operator is loaded"""
        self.accounts_dict = {}

    def flat_map(self, data):
        """Runs every time a new data arrives from either stream since they have been merged by 'union'"""
        # start account at $0 for non-existing keys
        data_dict = json.loads(data)
        account_name = data_dict["name"]
        if account_name not in self.accounts_dict:
            self.accounts_dict.update({account_name: {"amount": 0, "status": "active"}})

        # check if account is inactive
        current_status_state = self.accounts_dict[account_name]["status"]
        if current_status_state == "inactive":
            yield (
                f"{account_name} -> ${self.accounts_dict[account_name]['amount']}[inactive], "
                f"StatusState: {self.accounts_dict[account_name]['status']}, "
                f"State: {self.accounts_dict}"
            )

        elif "add_amount" in data_dict:
            # update the total amount
            current_amount = self.accounts_dict[account_name]["amount"]
            current_status = self.accounts_dict[account_name]["status"]
            amount_to_add = data_dict["add_amount"]
            new_amount = current_amount + amount_to_add

            # update the accounts dict
            self.accounts_dict.update({account_name: {"amount": new_amount, "status": current_status}})

            yield f"{account_name}: ${self.accounts_dict[account_name]['amount']}, State: {self.accounts_dict}"

        else:
            # update the status
            current_amount = self.accounts_dict[account_name]["amount"]
            new_status = data_dict["status"]
            self.accounts_dict.update({account_name: {"amount": current_amount, "status": new_status}})

            yield f"{account_name}: ${self.accounts_dict[account_name]['amount']}, State: {self.accounts_dict}"


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
        .union(status_ds)  # stream with data format {"name": "elon", "status": "inactive"}
        # union merges the streams such that the operator that follows sees the data as coming from
        # one source; all data goes through the single "flat_map" function above
        .flat_map(Account())
        .print()
    )
    env.execute()


if __name__ == "__main__":
    main()
