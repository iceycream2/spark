from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 创建SparkSession对象
spark = SparkSession.builder.appName("survived_prediction").getOrCreate()

# 读取csv文件
df = spark.read.csv("file:///root/py_case/spark_5/titanic.csv", header=True, inferSchema=True)

# 1. 移除空值列
df = df.dropna()

# 2. 转换Sex和Embarked字段为整数值
sexIdx = StringIndexer(inputCol='Sex', outputCol='SexIndex')
sexEncoded = OneHotEncoder(inputCol='SexIndex', outputCol='SexVec')

embarkIdx = StringIndexer(inputCol='Embarked', outputCol='EmbarkIndex')
embarkEncoded = OneHotEncoder(inputCol='EmbarkIndex', outputCol='EmbarkVec')

# 合并处理步骤到Pipeline中
pipeline = Pipeline(stages=[sexIdx, sexEncoded, embarkIdx, embarkEncoded])

# 在Pipeline中应用转换
pipeline_model = pipeline.fit(df)
df_transformed = pipeline_model.transform(df)

# 特征向量化
assembler = VectorAssembler(inputCols=['Pclass', 'SexVec', 'Age', 'SibSp', 'Parch', 'Fare', 'EmbarkVec'], outputCol='features')
final_data = assembler.transform(df_transformed)

# 将数据集分成训练集和测试集
(training_data, test_data) = final_data.randomSplit([0.7, 0.3])

# 训练逻辑回归模型
lr = LogisticRegression(featuresCol='features', labelCol='Survived')
lr_model = lr.fit(training_data)

# 预测测试集数据
predictions = lr_model.transform(test_data)

# 评估预测结果
evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='Survived')
accuracy = evaluator.evaluate(predictions)
print('Accuracy:', accuracy)