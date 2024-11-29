import os
import sys
python_path=sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf=SparkConf().setAppName("first").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
print("====started=====")
## SQL PREPARATION



print("========= DATA PREPARATION======")

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
#df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
#df1.show()






data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
#cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
#prod.show()


# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")
'''

print("=========DONE GOOD TO GO BELOW======")

print("=====spark sql exercise=====")
print("select id,tdate from df")
spark.sql("select id,tdate from df").show()
print("select category='exercise'")
spark.sql("select * from df where category='Exercise'").show()
print("select category=exercise and spendby=cash,select  id,tdate,category,spendby")
spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()
print("select category=exercise,gymnastics")
spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
print("product contains gymnastics")
spark.sql("select * from df where product like '%Gymnastics%'").show()
print("category not equals exercise")
spark.sql("select * from df where category != 'Exercise'").show()
print("category not equals exercise,gymnastics")
spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
print("product is null")
spark.sql("select * from df where product is null").show()
print("product is not null")
spark.sql("select * from df where product is not null").show()
print("maximum id")
spark.sql("select max(id) as id_max from df").show()
print("minimum id")
spark.sql("select min(id) as id_min from df").show()
print("count rows")
spark.sql("select count(1) from df").show()
print("add extra column status where cash=1,card=0")
spark.sql("select *,case when spendby='cash' then 1 else 0 end as status from df").show()
print("concat data")
spark.sql("select id,category,concat(id,'-',category) as condata from df").show()
print("concat_ws function")
spark.sql("select id,category,product,concat_ws('-',id,category,product) as condata from df").show()

print("lower case data")
spark.sql("select category,lower(category) from df").show()
print("upper case data")
spark.sql("select category,upper(category) from df").show()
print("ceil")
spark.sql("select amount,ceil(amount) from df").show()
print("floor")
spark.sql("select amount,floor(amount) from df").show()
print("replacing nulls with na")
spark.sql("select product,coalesce(product,'NA') as null_rep from df").show()
print("trim space")
spark.sql("select trim(category) from df").show()
print("distinct")
spark.sql("select distinct category from df").show()
'''
print("distinct multiple columns")
spark.sql("select distinct category,spendby from df").show()