from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext


class Account(FlatMapFunction):
    def open(self, ctx: RuntimeContext):
        """Runs only the first time the operator is loaded"""
        self.accounts_dict = {}

    def flat_map(self, data):
        """Runs every time a new data arrives"""
        # start account at $0 for non-existing keys
        account_name = data["name"]
        if account_name not in self.accounts_dict:
            self.accounts_dict.update({account_name: 0})

        # update the total amount
        current_amount = self.accounts_dict[account_name]
        amount_to_add = data["add_amount"]
        new_amount = current_amount + amount_to_add

        # update the accounts dict
        self.accounts_dict.update({account_name: new_amount})

        yield f"{account_name}: ${self.accounts_dict[account_name]}, State: {self.accounts_dict}"


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
        .flat_map(Account())
        .print()
    )
    env.execute()


if __name__ == "__main__":
    main()
