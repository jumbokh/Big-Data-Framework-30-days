from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.window import Slide
from pyflink.table import DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udaf

from collections import Counter

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

@udaf(result_type=DataTypes.ARRAY(DataTypes.STRING()), func_type="pandas")
def hot_movies(movie_ids):
    movie_counts = Counter(movie_ids)
    return sorted(movie_counts, key=lambda x: movie_counts[x], reverse=True)[:5]

# source tables
t_env.execute_sql("""
    CREATE TABLE ratings (
        `userId` INT,
        `movieId1` INT,
        `rating` DOUBLE,
        `timestamp` BIGINT,
        `event_time` as TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
        WATERMARK FOR `event_time` AS `event_time`
    ) WITH (
        'connector' = 'filesystem',
        'path' = '../../data/ml-latest-small/ratings.csv',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'                             
    )
""")

t_env.execute_sql("""
    CREATE TABLE movies (
        `movieId2` INT,
        `title` STRING,
        `genres` STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '../../data/ml-latest-small/movies.csv',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'    
    )
""")

# sink table
t_env.execute_sql("""
    CREATE TABLE output (
        `date` TIMESTAMP(3),
        `hot_movies` ARRAY<STRING>
    ) WITH (
        'connector' = 'print'                       
    )
""")


results = t_env.from_path("ratings") \
    .where(col('rating') >= 4.0) \
    .join(t_env.from_path("movies"), col('movieId1') == col('movieId2')) \
    .window(Slide.over(lit(300).days).every(lit(100).days).on(col('event_time')).alias('w'))\
    .group_by(col('w'))\
    .select(col('w').end, hot_movies(col('title')))

results.execute_insert('output') \
    .wait()