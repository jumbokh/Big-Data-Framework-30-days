from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
spark = SparkSession.builder.appName("Collaborative-Filtering: Spark MLlib").enableHiveSupport().getOrCreate()

# read tables
data = spark.table('ratings')
data.show(5)

# init model
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")

# train test split
(train_data, test_data) = data.randomSplit([0.8, 0.2])

# train model
model = als.fit(train_data)

# test model
predictions = model.transform(test_data)

# evaulate model
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) = {rmse}")

# show results
userRecs = model.recommendForAllUsers(10)
userRecs.show(5)

itemRecs = model.recommendForAllItems(10)
itemRecs.show(5)

# close connection
spark.stop()
