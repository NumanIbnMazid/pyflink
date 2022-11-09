from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class Account(FlatMapFunction):
    def open(self, ctx: RuntimeContext):
        """Runs only the first time the operator is loaded"""
        descriptor = ValueStateDescriptor(
            "accounts",  # state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.accounts_state = ctx.get_state(descriptor)  # state handle; always starts off as None

    def flat_map(self, data):
        """Runs every time a new data arrives"""
        # start account at $0 for non-existing keys
        current_state = self.accounts_state.value()
        account_name = data["name"]
        if current_state is None:
            self.accounts_state.update({account_name: 0})

        # update the total amount
        current_amount = self.accounts_state.value()[account_name]
        amount_to_add = data["add_amount"]
        new_amount = current_amount + amount_to_add

        # update the state
        self.accounts_state.update({account_name: new_amount})

        yield f"{account_name}: ${self.accounts_state.value()[account_name]}, AccountsState: {self.accounts_state.value()}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    input_data = [
        {"name": "elon", "add_amount": 5000},
        {"name": "elon", "add_amount": 1000},
        {"name": "bezos", "add_amount": 6000},
        {"name": "warren", "add_amount": 2000},
        {"name": "bezos", "add_amount": 4000},
        {"name": "warren", "add_amount": 3000},
        {"name": "elon", "add_amount": 5500},
        {"name": "bezos", "add_amount": 3100},
    ]
    ds = env.from_collection(input_data)

    # expected final amounts
    # elon: $11500
    # bezos: $13100
    # warren: $5000

    # topology definition
    (
        ds
        .key_by(lambda data: data["name"])  # produces a KeyedDataStream
        # a KeyedDataStream is required whenever we want to use a KeyedState in the next operator
        .flat_map(Account())
        .print()
    )
    env.execute()


if __name__ == "__main__":
    main()
