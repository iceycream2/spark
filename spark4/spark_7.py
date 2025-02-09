from pyspark import SparkContext, SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
def f(x):
    rel = {}
    rel['userId'] = int(x[0])
    rel['movieId'] = int(x[1])
    rel['rating'] = float(x[2])
    rel['timestamp'] = float(x[3])
    return rel

conf = SparkConf().setAppName("rdd_tutorial").setMaster("local[4]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

ratings = sc.textFile("file:///root/py_case/spark_7/sample_movielens_ratings.txt", 3).map(lambda line: line.split('::')).map(lambda p: Row(**f(p))).toDF()
#ratings.show()
training, test = ratings.randomSplit([0.8,0.2])
alsExplicit  = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
alsImplicit = ALS(maxIter=5, regParam=0.01, implicitPrefs=True,userCol="userId", itemCol="movieId", ratingCol="rating")
modelExplicit = alsExplicit.fit(training)
modelImplicit = alsImplicit.fit(training)
predictionsExplicit = modelExplicit.transform(test)
predictionsImplicit = modelImplicit.transform(test)
evaluator = RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

rmseExplicit = evaluator.evaluate(predictionsExplicit)
print("Explicit: Root-mean-square error =", rmseExplicit)
# 计算隐式反馈的RMSE
rmseImplicit = evaluator.evaluate(predictionsImplicit)
print("Implicit: Root-mean-square error =", rmseImplicit)