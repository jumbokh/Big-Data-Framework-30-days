from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col


# 1. Create TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

## Set Dependency (Download here: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/)
t_env.get_config().get_configuration().set_string("pipeline.jars", f"file:///home/mengchiehliu/projects/big_data_30_days/flink-sql-connector-kafka-1.17.1.jar")


# 2. Create Source Table
table = t_env.execute_sql("""
    CREATE TABLE my_source (
        `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
        `partition` BIGINT METADATA VIRTUAL,
        `offset` BIGINT METADATA VIRTUAL,
        `message` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'flink-source',
        'properties.bootstrap.servers' = 'localhost:9092',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'raw'                            
    )
""")
table = t_env.from_path("my_source")


# 3. Create Sink Table
t_env.execute_sql("""
    CREATE TABLE my_sink (
        event_time TIMESTAMP(3),
        kafka_partition BIGINT,
        kafka_offset BIGINT, 
        message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'flink-sink',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'csv'
    )
""")
                  
# 4. Query
sink_table = table.select(col('event_time'), col('partition'), col('offset'), col('message').upper_case)

# 5. Emit
sink_table.execute_insert("my_sink").wait()
