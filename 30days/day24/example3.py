from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types

# 1. Environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
t_env = StreamTableEnvironment.create(env)

# 2. DataStream
t_env.execute_sql("""
        CREATE TABLE my_source (
          a INT,
          b VARCHAR
        ) WITH (
          'connector' = 'datagen',
          'number-of-rows' = '10'
        )
    """)

ds = t_env.to_append_stream(
    t_env.from_path('my_source'),
    Types.ROW([Types.INT(), Types.STRING()]))

# 3. Transform
transformed_ds = ds.map(lambda x: (x[0], x[1][:10]))

# 4. Sink
transformed_ds.print()

# 5. Execute
env.execute("Example 3")