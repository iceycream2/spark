import pyspark
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import Row

from pyspark import SparkConf, SparkContext
spark = SparkSession.builder.appName("test").config("master","local[4]") .getOrCreate()
sc = spark.sparkContext
file='file:///root/py_case/spark_3/data03.txt'
rdd=sc.textFile(file,2)
df = rdd.map(lambda line: json.loads(line)) \
              .map(lambda data: Row(id=data.get("id"), name=data.get("name"), age=data.get("age", None))) \
              .filter(lambda x: x is not None) \
              .toDF()
json_rdd = rdd.map(lambda line: json.loads(line))

# 转化为 DataFrame
df = spark.createDataFrame(json_rdd)
df.createOrReplaceTempView("temp_view")
#
print("打印全部数据\n")
spark.sql("select *from temp_view").show()#打印全部数据
#
print("去除重复数据\n")
spark.sql("select distinct *from temp_view ").show()#去除重复数据
#
print("去除id字段\n")
spark.sql("SELECT age,name FROM temp_view").show()#去除id字段
#
print("age>30\n")
spark.sql("SELECT * FROM temp_view WHERE age > 30").show()#age>30
#
print("age分组\n")
spark.sql("select age, first(id) as id, first(name) as name from temp_view group by age").show()#age分组
#
print("name升序\n")
spark.sql("select age,id,name from temp_view order by name asc").show()#name升序
#
print("前三行\n")
df.show(3)#前三行
#
print("查询name并取名username\n")
spark.sql("select name as username from temp_view ").show()#查询name并取名username
#
print("平均值\n")
spark.sql("select avg(age) as avg_age from temp_view").show()#平均值
#
print("最小值\n")
spark.sql("select min(age) as min_age from temp_view").show()#最小值
#
#写入全部
allData=spark.sql("select *from temp_view")
allData.write.format("jdbc").options(
    url="jdbc:mysql://192.168.153.100:3306/spark",
    driver="com.mysql.jdbc.Driver",
    dbtable="stu",
    user="root",
    useSSL="false",
    verifyServerCertificate="false",
    password="123456"
).mode("overwrite").save()
