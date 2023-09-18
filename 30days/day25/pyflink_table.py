from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
import pandas as pd
from pyflink.table.expressions import col


# 1. Create TableEnvironment
## env_settings = EnvironmentSettings.in_streaming_mode()
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Create Source Table
## 1. List Array
table = t_env.from_elements(
    [(1, 'hadoop'), (2, 'spark'), (3, 'flink')], 
    ['id', 'data'])

print("\n1. List Array")
table.execute().print()

## 2. DDL Statements 
table = t_env.execute_sql("""
    CREATE TABLE my_source (
        id INT, 
        score INT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='3',
        'fields.score.kind'='random',
        'fields.score.min'='1',
        'fields.score.max'='100',
        'number-of-rows' = '10'
    )
""")
table = t_env.from_path("my_source")

print("\n2. DDL Statements")
table.execute().print()


## 3. TableDescriptor
t_env.create_temporary_table(
    'my_source',
    TableDescriptor.for_connector('datagen')
        .schema(Schema.new_builder()
                .column('id', DataTypes.BIGINT())
                .column('score', DataTypes.BIGINT())
                .build())
        .option('fields.id.kind', 'sequence')
        .option('fields.id.start', '1')
        .option('fields.id.end', '3')
        .option('fields.score.kind', 'random')
        .option('fields.score.min', '1')
        .option('fields.score.max', '100')
        .option('number-of-rows', '10')
        .build()
)
table = t_env.from_path("my_source")

print("\n3. TableDescriptor")
table.execute().print()

## 4. Pandas
df = pd.DataFrame.from_dict({
    'id':[1,2,3], 
    'data': ['hadoop', 'spark', 'flink']
    })

table = t_env.from_pandas(df)
print("\n4. Pandas")
table.execute().print()

# 3. Create Sink Table
t_env.execute_sql("""
    CREATE TABLE my_sink (
        id BIGINT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")
                  
# 4. Query
sink_table = table.select(col("id")*10, col("data"))

# 5. Emit
print("\nEmitted table")
sink_table.execute_insert("my_sink").wait()
# t_env.create_temporary_view('sink_table', sink_table)
# t_env.execute_sql("INSERT INTO my_sink SELECT * FROM sink_table").wait()