# create SparkSession
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("PySpark_SQL") \
    .enableHiveSupport() \
    .getOrCreate()

# Drop table if exist
spark.sql("DROP TABLE IF EXISTS users")
spark.sql("DROP TABLE IF EXISTS jobs")

# Create tables
spark.sql("CREATE TABLE users (id int, name string)")
spark.sql("CREATE TABLE jobs (id int, salary int)")

# Insert data
## write data
query = """
    INSERT INTO jobs VALUES
    (1, 110000), (2, 50000), (3, 60000),
    (7, 90000), (8, 30000), (9, 100000)
    """
spark.sql(query)

## write from local file
df = spark.read.csv("users.txt", header=False, inferSchema=True) # inferSchema: detect dtype
df.createOrReplaceTempView("temp_table")
spark.sql("INSERT INTO TABLE users SELECT * FROM temp_table")

# selcet, join, where
query = """
    SELECT users.id, users.name, jobs.salary 
    FROM users 
    LEFT JOIN jobs
    ON users.id=jobs.id 
    WHERE jobs.salary >= 100000 
    """
df = spark.sql(query)

# show results
df.show()
spark.stop()