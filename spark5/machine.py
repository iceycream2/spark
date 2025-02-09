from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.getOrCreate()

data = spark.read.json("file:///root/py_case/spark_8/resources/*.txt")

sales_num_column = data.select("evaluations.element.data.page_content.element.quality_rank")

sales_num_column.show()