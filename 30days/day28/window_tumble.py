from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().get_configuration().set_string("execution.rowtime-timestamp-type", "from-source")

t_env.execute_sql("""
    CREATE TABLE my_source (
        event_time TIMESTAMP(3),
        `value` DOUBLE,
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'test_data.csv',
        'format' = 'csv'
    )
""")

t_env.execute_sql("""
    CREATE TABLE my_avg (
        window_start_time TIMESTAMP(3),
        avg_value DOUBLE
    ) WITH (
        'connector' = 'print'
    )
""")

t_env.from_path('my_source')\
    .window(Tumble.over(lit(10).seconds).on(col('event_time')).alias('w'))\
    .group_by(col("w")) \
    .select(col("w").start.alias('window_start_time'), col('value').avg.alias('avg_value'))\
    .execute_insert("my_avg")\
    .wait()

