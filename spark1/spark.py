from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("rdd_tutorial").setMaster("local[4]")
sc = SparkContext(conf=conf)
file= "data01.txt"
file1="A.txt"
file2="B.txt"
rdd1=sc.textFile(file1,3)
rdd2=sc.textFile(file2,3)
lines = rdd1.union(rdd2)
rdd = sc.textFile(file,3)
data=rdd.map(lambda x:x.split(","))
#学生人数
student=data.map(lambda x:x[0]).distinct()
#课程数量
lesson=data.map(lambda x:x[1]).distinct()
#tom的平均分
ave_tom=data.filter(lambda x:x[0]=='Tom').map(lambda x:int(x[2])).sum()/data.filter(lambda x:x[0]=='Tom').count()
#每名同学的选修课程数
count=data.map(lambda x:(x[0],1)).reduceByKey(lambda x,y :x+y)
#数据库的选修人数
people=data.filter(lambda x:x[1]=='DataBase').count()
#各个课程的平均分
subject=data.groupBy(lambda x: x[1])
totals_score=subject.map(lambda x:(x[0],sum(int(score) for _,_,score in x[1])))
totals_stu=subject.map(lambda x:(x[0],len(x[1])))
ave_scores = totals_score.join(totals_stu).mapValues(lambda x:x[0]/x[1])
# 文件合并
result=lines.map(lambda x:x.split('\t')).map(lambda pair: (pair[0], pair[1])).distinct().sortBy(lambda x:x[0]).map(lambda z:'\t'.join([z[0],z[1]]))
result.coalesce(1).saveAsTextFile("C.txt")
print("学生人数为%d"%len(student.collect()))
print("课程数量为%d"%len(lesson.collect()))
print("tom的平均分为%.2f"%ave_tom)
print("每名同学的选修课程数")
print(count.collect())
print("数据库的选修人数%d"%people)
print("各个课程的平均分")
print(ave_scores.collect())