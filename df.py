import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)


if __name__ == '__main__':
   print("creating spark session")


   spark = SparkSession \
           .builder \
           .appName("debu application") \
           .config("spark.sql.shuffle.partitions", 3) \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')


   # creating a df using RDD


   rdd = spark.sparkContext.parallelize(range(1, 10))
   df = rdd.map(lambda x: (x, x * 2))
   #df.take(2):  [(1, 2), (2, 4)] 
   df = rdd.map(lambda x: (x, x * 2)).toDF(["id", "value"])
   df.show()
   # +---+-----+                                                                    
   # | id|value|
   # +---+-----+
   # |  1|    2|
   # |  2|    4|
   # |  3|    6|
   # |  4|    8|
   # |  5|   10|
   # |  6|   12|
   # |  7|   14|
   # |  8|   16|
   # |  9|   18|
   # +---+-----+




   rdd1 = spark.sparkContext.parallelize(range(1, 10))
   df1 = rdd1.map(lambda x: (x, "row: {}".format(x))).toDF(["id",  "value"])
   df1.show()
   # +---+------+
   # | id| value|
   # +---+------+
   # |  1|row: 1|
   # |  2|row: 2|
   # |  3|row: 3|
   # |  4|row: 4|
   # |  5|row: 5|
   # |  6|row: 6|
   # |  7|row: 7|
   # |  8|row: 8|
   # |  9|row: 9|
   # +---+------+




   df1.foreach(lambda x: print(x))
   # Row(id=8, value='row: 8')
   # Row(id=9, value='row: 9')
   # Row(id=4, value='row: 4')
   # Row(id=7, value='row: 7')
   # Row(id=5, value='row: 5')
   # Row(id=6, value='row: 6')
   # Row(id=3, value='row: 3')
   # Row(id=1, value='row: 1')
   # Row(id=2, value='row: 2')


   print("yes", df1.collect())


   # [Row(id=1, value='row: 1'),
   #  Row(id=2, value='row: 2'),
   #  Row(id=3, value='row: 3'),
   #  Row(id=4, value='row: 4'),
   #  Row(id=5, value='row: 5'),
   #  Row(id=6, value='row: 6'),
   #  Row(id=7, value='row: 7'),
   #  Row(id=8, value='row: 8'),
   #  Row(id=9, value='row: 9')]
  

   # creating a df using RDD, and its thier own schema
   from pyspark.sql import Row
   from pyspark.sql.types import StructType, StructField, IntegerType, StringType
   rdd2 = spark.sparkContext.parallelize([(Row(1, "abc", 20),
                                        Row(2, "def", 30),
                                        Row(3, "ghi", 40))])
   schema = StructType([
       StructField("id", IntegerType(), True),
       StructField("name", StringType(), True),
       StructField("marks", IntegerType(), True)
   ])


   print("Creating DataFrame with custom schema")
   df2 = spark.createDataFrame(rdd2, schema)
   df2.printSchema()
#     df2.show()
#     df2.foreach(lambda x: print(x))


   # creating a df using file
   df3 = spark.read.csv("/Users/gauravmishra/Desktop/SparkDF/Datasets/customers.csv", header=True, inferSchema=True)
   df3.show()
   print(type(df3), type(df3.show(10)), type(df3.head(5)), type(df3.take(5)), type(df3.columns), type(df3.dtypes))
   # <class 'pyspark.sql.classic.dataframe.DataFrame'> <class 'NoneType'> <class 'list'> <class 'list'> <class 'list'> <class 'list'>
   print("head 5: ", df3.head(5))
   # [Row(customer_id=1, customer_fname='Richard', customer_lname='Hernandez', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='6303 Heather Plaza', customer_city='Brownsville', customer_state='TX', customer_zipcode=78521), Row(customer_id=2, customer_fname='Mary', customer_lname='Barrett', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='9526 Noble Embers Ridge', customer_city='Littleton', customer_state='CO', customer_zipcode=80126), Row(customer_id=3, customer_fname='Ann', customer_lname='Smith', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='3422 Blue Pioneer Bend', customer_city='Caguas', customer_state='PR', customer_zipcode=725), Row(customer_id=4, customer_fname='Mary', customer_lname='Jones', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='8324 Little Common', customer_city='San Marcos', customer_state='CA', customer_zipcode=92069), Row(customer_id=5, customer_fname='Robert', customer_lname='Hudson', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='10 Crystal River Mall ', customer_city='Caguas', customer_state='PR', customer_zipcode=725)]
   print("take 5: ", df3.take(5))
   # [Row(customer_id=1, customer_fname='Richard', customer_lname='Hernandez', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='6303 Heather Plaza', customer_city='Brownsville', customer_state='TX', customer_zipcode=78521), Row(customer_id=2, customer_fname='Mary', customer_lname='Barrett', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='9526 Noble Embers Ridge', customer_city='Littleton', customer_state='CO', customer_zipcode=80126), Row(customer_id=3, customer_fname='Ann', customer_lname='Smith', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='3422 Blue Pioneer Bend', customer_city='Caguas', customer_state='PR', customer_zipcode=725), Row(customer_id=4, customer_fname='Mary', customer_lname='Jones', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='8324 Little Common', customer_city='San Marcos', customer_state='CA', customer_zipcode=92069), Row(customer_id=5, customer_fname='Robert', customer_lname='Hudson', customer_email='XXXXXXXXX', customer_password='XXXXXXXXX', customer_street='10 Crystal River Mall ', customer_city='Caguas', customer_state='PR', customer_zipcode=725)]
   print("columns: ", df3.columns)
   # ['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 'customer_password', 'customer_street', 'customer_city', 'customer_state', 'customer_zipcode']
   print("dtypes: ", df3.dtypes)
   # [('customer_id', 'int'), ('customer_fname', 'string'), ('customer_lname', 'string'), ('customer_email', 'string'), ('customer_password', 'string'), ('customer_street', 'string'), ('customer_city', 'string'), ('customer_state', 'string'), ('customer_zipcode', 'int')]
   print("Number of rows in df3: ", df3.count())
   print("Number of columns in df3: ", len(df3.columns))
   df3.printSchema()
  


   # how to print few lines with foreach function
   #df3.take(3).foreach(lambda x: print(x)) # this will not work, as take returns a list
   # in spark df, each record is a spark sql Row object, and foreach is an action that applies a function to each element of the RDD.
   # so, we can use foreach on the DataFrame directly, but not on the result
   #df3.foreach(lambda x: print(x)) # this will work, as foreach is an action and will print each row


   import pyspark.sql.functions as sf
   data = spark.createDataFrame([(5, 1, -1)], ['start', 'stop', 'step'])
   data.select(sf.sequence(data.start, data.stop, data.step)).show()


   #Create a DataFrame from a list of tuples.
   print("df creation from a list of tuples", spark.createDataFrame([('Alice', 1)]).toDF("name", "score").show())


   #Create a DataFrame from a list of dictionaries.
   d = [{'name': 'Alice', 'age': 1}]
   print("Spark df creation by using list of dict", spark.createDataFrame(d).show())


   #Create a DataFrame with column names specified.
   print("saprk df creation by using list of tupes", spark.createDataFrame([('Alice', 1), ('John', 2)], ['name', 'age']).show())


   from pyspark.sql.types import *
   schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)])
   spark.createDataFrame([('Alice', 1)], schema).show()


   #Create a DataFrame with the schema in DDL formatted string.
   spark.createDataFrame([('Alice', 1)], "name: string, age: int").show()


   #Create a DataFrame from a pandas DataFrame.
   df1 = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [1, 2]})
   print("pandas df", df1)
   print("data frame by using pandas to spark", spark.createDataFrame(df1).show())


   #Create an empty DataFrame. When initializing an empty DataFrame in PySpark,
   # it's mandatory to specify its schema, as the DataFrame lacks data from which the schema can be inferred.
   spark.createDataFrame([], "name: string, age: int").show()


   print(df3.groupBy("customer_state").count().show())


   # create a df from a local list
   x = [1, 2, 3, 4, 5]
   DF4 = spark.createDataFrame(x, IntegerType())
   print("DF4", DF4.show())




   # create a rdd from a local list
   RDD1 = spark.sparkContext.parallelize(x)
   print("RDD1", RDD1.collect())


   # we can creata a df using spark or any table
   df3.createOrReplaceTempView("customers")
   DF5 = spark.sql("SELECT * FROM customers limit 5")
   print("DF5", DF5.show())
   # +-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
   # |customer_id|customer_fname|customer_lname|customer_email|customer_password|     customer_street|customer_city|customer_state|customer_zipcode|
   # +-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
   # |          1|       Richard|     Hernandez|     XXXXXXXXX|        XXXXXXXXX|  6303 Heather Plaza|  Brownsville|            TX|           78521|
   # |          2|          Mary|       Barrett|     XXXXXXXXX|        XXXXXXXXX|9526 Noble Embers...|    Littleton|            CO|           80126|
   # |          3|           Ann|         Smith|     XXXXXXXXX|        XXXXXXXXX|3422 Blue Pioneer...|       Caguas|            PR|             725|
   # |          4|          Mary|         Jones|     XXXXXXXXX|        XXXXXXXXX|  8324 Little Common|   San Marcos|            CA|           92069|
   # |          5|        Robert|        Hudson|     XXXXXXXXX|        XXXXXXXXX|10 Crystal River ...|       Caguas|            PR|             725|
   # +-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+




   # coverting RDD to DataFrame
   data_rdd = spark.sparkContext.textFile("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/orders.csv")
   print("data_rdd", data_rdd.take(5))
   # ['1,2013-07-25 00:00:00.0,11599,CLOSED', '2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT', '3,2013-07-25 00:00:00.0,12111,COMPLETE', '4,2013-07-25 00:00:00.0,8827,CLOSED', '5,2013-07-25 00:00:00.0,11318,COMPLETE']


   # Split each line by comma and convert to tuple
   data_tuples = data_rdd.map(lambda line: tuple(line.split(',')))
   data_tuples = data_tuples.map(lambda x: (int(x[0]), x[1], int(x[2]), x[3]))  # Convert types as needed
   print("data_tuples", data_tuples.take(5))
   # [(1, '2013-07-25 00:00:00.0', 11599, 'CLOSED'), (2, '2013-07-25 00:00:00.0', 256, 'PENDING_PAYMENT'), (3, '2013-07-25 00:00:00.0', 12111, 'COMPLETE'), (4, '2013-07-25 00:00:00.0', 8827, 'CLOSED'), (5, '2013-07-25 00:00:00.0', 11318, 'COMPLETE')]


   data_schema = 'order_id long, order_date string, customer_id long, order_status string'


   orders_df_from_rdd = spark.createDataFrame(data_tuples, schema=data_schema)
   print("orders_df_from_rdd", orders_df_from_rdd.show(5))


   # +--------+--------------------+-----------+---------------+
   # |order_id|          order_date|customer_id|   order_status|
   # +--------+--------------------+-----------+---------------+
   # |       1|2013-07-25 00:00:...|      11599|         CLOSED|
   # |       2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|
   # |       3|2013-07-25 00:00:...|      12111|       COMPLETE|
   # |       4|2013-07-25 00:00:...|       8827|         CLOSED|
   # |       5|2013-07-25 00:00:...|      11318|       COMPLETE|
   # +--------+--------------------+-----------+---------------+




   # creating a df using list of tuples by giving column names explictly
   xyz = [(1, '2013-07-25 00:00:00.0', 11599, 'CLOSED'), (2, '2013-07-25 00:00:00.0', 256, 'PENDING_PAYMENT'), (3, '2013-07-25 00:00:00.0', 12111, 'COMPLETE'), (4, '2013-07-25 00:00:00.0', 8827, 'CLOSED'), (5, '2013-07-25 00:00:00.0', 11318, 'COMPLETE')]


   df_xyz = spark.createDataFrame(xyz, schema=['order_id', 'order_date', 'customer_id', 'order_status'])
              
   print("df_xyz", df_xyz.show(5))
   # +--------+--------------------+-----------+---------------+
   # |order_id|          order_date|customer_id|   order_status|
   # +--------+--------------------+-----------+-----------s----+
   # |       1|2013-07-25 00:00:...|      11599|         CLOSED|
   # |       2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|
   # |       3|2013-07-25 00:00:...|      12111|       COMPLETE|
   # |       4|2013-07-25 00:00:...|       8827|         CLOSED|
   # |       5|2013-07-25 00:00:...|      11318|       COMPLETE|
   # +--------+--------------------+-----------+---------------+


   abc = [(1, '2013-07-25 00:00:00.0', 11599, 'CLOSED'), (2, '2013-07-25 00:00:00.0', 256, 'PENDING_PAYMENT'), (3, '2013-07-25 00:00:00.0', 12111, 'COMPLETE'), (4, '2013-07-25 00:00:00.0', 8827, 'CLOSED'), (5, '2013-07-25 00:00:00.0', 11318, 'COMPLETE')]


   df_abc = spark.createDataFrame(abc, schema=('order_id', 'order_date', 'customer_id', 'order_status'))
              
   print("df_abc", df_abc.show(5))


   # +--------+--------------------+-----------+---------------+
   # |order_id|          order_date|customer_id|   order_status|
   # +--------+--------------------+-----------+---------------+
   # |       1|2013-07-25 00:00:...|      11599|         CLOSED|
   # |       2|2013-07-25 00:00:...|        256|PENDING_PAYMENT|
   # |       3|2013-07-25 00:00:...|      12111|       COMPLETE|
   # |       4|2013-07-25 00:00:...|       8827|         CLOSED|
   # |       5|2013-07-25 00:00:...|      11318|       COMPLETE|
   # +--------+--------------------+-----------+---------------+

