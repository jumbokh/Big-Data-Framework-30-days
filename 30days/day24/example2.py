from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.common.watermark_strategy import WatermarkStrategy

# 1. Environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(3)

# 2. DataStream
my_source = NumberSequenceSource(1, 10)
ds = env.from_source(
    source=my_source, 
    source_name='my_source',
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps())

# 3. Transform
transformed_ds = ds.map(lambda x: (x, x * 10))

# 4. Sink
transformed_ds.print()

# 5. Execute
env.execute("Example 2")