import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)





spark = SparkSession \
           .builder \
           .appName("weeek#8 application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

data = [{'name': ['Anima', 'Gaurav', 'Ashish', 'Tushar', 'Adity'], 'sirname':['Dixit', 'Mishra', 'Mishra', 'Mishra', 'Dixit']}]
df1 = spark.createDataFrame(data)
print(df1.head())
# Row(name=['Anima', 'Gaurav', 'Ashish', 'Tushar', 'Adity'], sirname=['Dixit', 'Mishra', 'Mishra', 'Mishra', 'Dixit'])
print(df1.show())
# +--------------------+--------------------+
# |                name|             sirname|
# +--------------------+--------------------+
# |[Anima, Gaurav, A...|[Dixit, Mishra, M...|
# +--------------------+--------------------+

data = [{'name': ['Anima'], 'sirname':['Dixit']}]
df2 = spark.createDataFrame(data)
print(df2.show())
# +-------+-------+
# |   name|sirname|
# +-------+-------+
# |[Anima]|[Dixit]|
# +-------+-------+

data = [('Anima', 'Dixit', 10, 2),
       ('Gaurav', 'Mishra', 20, 3),
       ('Ashish', 'Mishra', 30, 3),
       ('Tushar', 'Mishra', 40, 4),
       ('Aditya', 'Dixit', 50, 5)]
df = spark.createDataFrame(data, "name: string, sirname: string, marks:integer,grade:integer")
print(df.show())

# +------+-------+-----+-----+
# |  name|sirname|marks|grade|
# +------+-------+-----+-----+
# | Anima|  Dixit|   10|    2|
# |Gaurav| Mishra|   20|    3|
# |Ashish| Mishra|   30|    3|
# |Tushar| Mishra|   40|    4|
# |Aditya|  Dixit|   50|    5|
# +------+-------+-----+-----+

df.select("marks","grade", expr('marks + grade as new_col')).show()
# this will work

# +-----+-----+-------+
# |marks|grade|new_col|
# +-----+-----+-------+
# |   10|    2|     12|
# |   20|    3|     23|
# |   30|    3|     33|
# |   40|    4|     44|
# |   50|    5|     55|
# +-----+-----+-------+


# df.select("name","marks", expr('name' + 'marks')).show()
# this will not work

df.select(['marks', 'grade']).show()
# +-----+-----+
# |marks|grade|
# +-----+-----+
# |   10|    2|
# |   20|    3|
# |   30|    3|
# |   40|    4|
# |   50|    5|
# +-----+-----+

df['marks']
# will not work
df.marks
# will not work

df.select('marks', df.marks, df['marks']).show()
# this will work
# +-----+-----+-----+
# |marks|marks|marks|
# +-----+-----+-----+
# |   10|   10|   10|
# |   20|   20|   20|
# |   30|   30|   30|
# |   40|   40|   40|
# |   50|   50|   50|
# +-----+-----+-----+

df.select(col('marks'), expr('marks')).show()
# +-----+-----+
# |marks|marks|
# +-----+-----+
# |   10|   10|
# |   20|   20|
# |   30|   30|
# |   40|   40|
# |   50|   50|
# +-----+-----+

# df.select(Column('marks'))
# this will not work

df.select('marks', expr('marks + grade')).show()
from pyspark.sql.functions import * 
df.select(count("*").alias("row_cnt"),
         countDistinct("name").alias("unique_name"),
         sum("marks").alias("tot_marks"),
         avg('grade').alias("avg_grade")).show()
# +-------+-----------+---------+---------+
# |row_cnt|unique_name|tot_marks|avg_grade|
# +-------+-----------+---------+---------+
# |      5|          5|      150|      3.4|
# +-------+-----------+---------+---------+

df.selectExpr("count(*) as row_cnt", "count(Distinct name) as unique_name", "sum(marks) as tot_marks", "avg(grade) as avg_grade").show()
# +-------+-----------+---------+---------+
# |row_cnt|unique_name|tot_marks|avg_grade|
# +-------+-----------+---------+---------+
# |      5|          5|      150|      3.4|
# +-------+-----------+---------+---------+

df.createOrReplaceTempView("testing_df")

spark.sql("select count(*) as row_cnt, count(Distinct name) as unique_name, sum(marks) as tot_marks, avg(grade) as avg_grade from testing_df").show()
# +-------+-----------+---------+---------+
# |row_cnt|unique_name|tot_marks|avg_grade|
# +-------+-----------+---------+---------+
# |      5|          5|      150|      3.4|
# +-------+-----------+---------+---------+

df.select(df['marks'] == 20, df.grade).show()
# +------------+-----+
# |(marks = 20)|grade|
# +------------+-----+
# |       false|    2|
# |        true|    3|
# |       false|    3|
# |       false|    4|
# |       false|    5|
# +------------+-----+

df[df['marks'] == 20, df.grade].show()
# +------------+-----+
# |(marks = 20)|grade|
# +------------+-----+
# |       false|    2|
# |        true|    3|
# |       false|    3|
# |       false|    4|
# |       false|    5|
# +------------+-----+


df.selectExpr("sum(IF(sirname = 'Mishra', marks, null)) as new_col_1", "sum(IF(sirname == 'Mishra', marks, null)) as new_col_2").show()
# +---------+---------+
# |new_col_1|new_col_2|
# +---------+---------+
# |       90|       90|
# +---------+---------+

df.select('marks','grade',expr('marks + grade as new_col')).show()
# +-----+-----+-------+
# |marks|grade|new_col|
# +-----+-----+-------+
# |   10|    2|     12|
# |   20|    3|     23|
# |   30|    3|     33|
# |   40|    4|     44|
# |   50|    5|     55|
# +-----+-----+-------+

df.select('marks','grade',expr('marks + 2 as new_col_2')).show()
# +-----+-----+---------+
# |marks|grade|new_col_2|
# +-----+-----+---------+
# |   10|    2|       12|
# |   20|    3|       22|
# |   30|    3|       32|
# |   40|    4|       42|
# |   50|    5|       52|
# +-----+-----+---------+


df.select('name',expr('name + 2 as new_col_2')).show()
# +------+---------+
# |  name|new_col_2|
# +------+---------+
# | Anima|     null|
# |Gaurav|     null|
# |Ashish|     null|
# |Tushar|     null|
# |Aditya|     null|
# +------+---------+

df.select('name',expr('name * 2 as new_col_2')).show()
# +------+---------+
# |  name|new_col_2|
# +------+---------+
# | Anima|     null|
# |Gaurav|     null|
# |Ashish|     null|
# |Tushar|     null|
# |Aditya|     null|
# +------+---------+

df.select('name',expr('name + sirname as new_col_2')).show()
# here mathematical operators will not work between 2 string columns
# use concat for this
# +------+---------+
# |  name|new_col_2|
# +------+---------+
# | Anima|     null|
# |Gaurav|     null|
# |Ashish|     null|
# |Tushar|     null|
# |Aditya|     null|
# +------+---------+

df.select('name',expr('concat(name, sirname) as new_col_2')).show()
# +------+------------+
# |  name|   new_col_2|
# +------+------------+
# | Anima|  AnimaDixit|
# |Gaurav|GauravMishra|
# |Ashish|AshishMishra|
# |Tushar|TusharMishra|
# |Aditya| AdityaDixit|
# +------+------------+

spark.sql("select name + sirname as new_col, marks + grade as new_col_1 from testing_df").show()
# +-------+---------+
# |new_col|new_col_1|
# +-------+---------+
# |   null|       12|
# |   null|       23|
# |   null|       33|
# |   null|       44|
# |   null|       55|
# +-------+---------+

#grouping

df.groupBy("sirname").agg(sum('marks').alias('tot_marks'), sum(expr("grade").alias("tot_grade"))).show()
# +-------+---------+-------------------------+
# |sirname|tot_marks|sum(grade AS `tot_grade`)|
# +-------+---------+-------------------------+
# | Mishra|       90|                       10|
# |  Dixit|       60|                        7|
# +-------+---------+-------------------------+


df.groupBy("sirname").agg({'marks':'sum', 'grade':'sum'}).show()
# for multi level group by use this: df.groupBy(['sirname', 'name'])
# +-------+----------+----------+
# |sirname|sum(grade)|sum(marks)|
# +-------+----------+----------+
# | Mishra|        10|        90|
# |  Dixit|         7|        60|
# +-------+----------+----------+

df.groupBy("sirname").agg({'marks':'sum', 'grade':'sum'}).toPandas().rename(columns = {'sum(grade)': 'tot_grade', 'sum(marks)':'tot_marks'})
# this is a pandas df output
# for multi level group by use this: df.groupBy(['sirname', 'name'])
# sirname	 tot_grade	tot_marks
# Mishra	     10	     90
# Dixit	          7	     60


df.groupBy(['sirname', 'name']).agg({'marks':'sum', 'grade':'sum'}).toPandas().rename(columns = {'sum(grade)': 'tot_grade', 'sum(marks)':'tot_marks'})

# sirname	name	tot_grade	tot_marks
# Mishra	Ashish	  3	         30
# Mishra	Gaurav	  3	         20
# Dixit	    Aditya	  5	         50
# Dixit	    Anima	  2	         10
# Mishra	Tushar	  4	         40


data_1 = spark.createDataFrame([{'name': 'A', 'marks': 10}], 'name string, marks int') 
data_1.show() 
# +----+-----+ 
# |name|marks| 
# +----+-----+ 
# |   A|   10| 
# +----+-----+ 

data_1 = spark.createDataFrame([{'name': ['A'], 'marks': [10]}]) 
data_1.show() 
# +-----+----+ 
# |marks|name| 
# +-----+----+ 
# | [10]| [A]| 
# +-----+----+ 


data_2 = spark.createDataFrame([ 
   {"name": "Alice", "age": 30, "city": "New York"}, 
   {"name": "Bob", "age": 24, "city": "London"}, 
   {"name": "Charlie", "age": 35, "city": "Paris"} 
]) 
data_2.show() 
data_2.take(2) 
# +---+--------+-------+ 
# |age|    city|   name| 
# +---+--------+-------+ 
# | 30|New York|  Alice| 
# | 24|  London|    Bob| 
# | 35|   Paris|Charlie| 
# +---+--------+-------+ 

# [Row(age=30, city='New York', name='Alice'), Row(age=24, city='London', name='Bob')] 


data_2 = spark.createDataFrame(( 
   {"name": "Alice", "age": 30, "city": "New York"}, 
   {"name": "Bob", "age": 24, "city": "London"}, 
   {"name": "Charlie", "age": 35, "city": "Paris"} 
)) 
data_2.show() 
data_2.take(2) 
# +---+--------+-------+ 
# |age|    city|   name| 
# +---+--------+-------+ 
# | 30|New York|  Alice| 
# | 24|  London|    Bob| 
# | 35|   Paris|Charlie| 
# +---+--------+-------+ 

# [Row(age=30, city='New York', name='Alice'), Row(age=24, city='London', name='Bob')] 

data_2 = spark.createDataFrame([ 
   ["Alice", 30, "New York"], 
   ["Bob",  24,  "London"], 
   ["Charlie",  35, "Paris"] 
], 'name string, age integer, city string') 
data_2.show() 
data_2.take(2) 
# +-------+---+--------+ 
# |   name|age|    city| 
# +-------+---+--------+ 
# |  Alice| 30|New York| 
# |    Bob| 24|  London| 
# |Charlie| 35|   Paris| 
# +-------+---+--------+ 

# [Row(name='Alice', age=30, city='New York'), Row(name='Bob', age=24, city='London')] 

data_2 = spark.createDataFrame(( 
   ["Alice", 30, "New York"], 
   ["Bob",  24,  "London"], 
   ["Charlie",  35, "Paris"]) 
, 'name string, age integer, city string') 
data_2.show() 
data_2.take(2) 
# +-------+---+--------+ 
# |   name|age|    city| 
# +-------+---+--------+ 
# |  Alice| 30|New York| 
# |    Bob| 24|  London| 
# |Charlie| 35|   Paris| 
# +-------+---+--------+ 

# [Row(name='Alice', age=30, city='New York'),  Row(name='Bob', age=24, city='London')] 

 

 