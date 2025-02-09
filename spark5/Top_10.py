from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.getOrCreate()

data = spark.read.json("file:///root/py_case/spark_8/resources/*.txt")

sales_num_column = data.select("data.name","data.pro_id","sales_num").orderBy(desc("sales_num"))

sales_num_column.show(10)