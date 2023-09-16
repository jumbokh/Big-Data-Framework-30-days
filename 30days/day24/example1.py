from pyflink.datastream import StreamExecutionEnvironment

# 1. Environment
env = StreamExecutionEnvironment.get_execution_environment()

# 2. DataStream
ds = env.from_collection([(1, "hadoop"), (2, "spark"), (3, "flink")])

# 3. Transform
transformed_ds = ds.map(lambda x: (x[0], x[1].upper()))

# 4. Sink
transformed_ds.print()

# 5. Execute
env.execute("Example 1")