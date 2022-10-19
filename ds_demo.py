import json

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def data_stream_api_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql("""
            CREATE TABLE my_source (
              a INT
            ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '10'
            )
        """)

    ds = t_env.to_append_stream(
        t_env.from_path('my_source'),
        Types.ROW([Types.INT()]))

    def split(s):
        splits = s[1].split("|")
        for sp in splits:
            yield s[0], sp

    def show(s):
        res = {'result': s[0]}
        yield json.dumps(res)

    #ds = ds.flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()]))
    ds = ds.flat_map(show, Types.STRING())
         

    t_env.execute_sql("""
            CREATE TABLE my_sink (
              a VARCHAR
            ) WITH (
              'connector' = 'print'
            )
        """)

    table = t_env.from_data_stream(ds)
    table_result = table.execute_insert("my_sink")

    # 1）wait for job finishes and only used in local execution, otherwise, it may happen that the script exits with the job is still running
    # 2）should be removed when submitting the job to a remote cluster such as YARN, standalone, K8s etc in detach mode
    table_result.wait()


if __name__ == '__main__':
    data_stream_api_demo()

