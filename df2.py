import pyspark, pandas as pd
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
import getpass, time
username = getpass.getuser()
print(username)

from pyspark.sql.types import *

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


    orders_df = spark.read.csv("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/orders.csv", inferSchema=True)

    print("table1", orders_df.show(5))

    # +---+-------------------+-----+---------------+
    # |_c0|                _c1|  _c2|            _c3|
    # +---+-------------------+-----+---------------+
    # |  1|2013-07-25 00:00:00|11599|         CLOSED|
    # |  2|2013-07-25 00:00:00|  256|PENDING_PAYMENT|
    # |  3|2013-07-25 00:00:00|12111|       COMPLETE|
    # |  4|2013-07-25 00:00:00| 8827|         CLOSED|
    # |  5|2013-07-25 00:00:00|11318|       COMPLETE|
    # +---+-------------------+-----+---------------+

    orders_df.printSchema()

    #     root
    # |-- _c0: integer (nullable = true)
    # |-- _c1: timestamp (nullable = true)
    # |-- _c2: integer (nullable = true)
    # |-- _c3: string (nullable = true)

    orders_schema = 'order_id long, order_date date, customer_id long, order_status string'

    orders_df1 = spark.read.csv("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/orders.csv", schema=orders_schema)
    print("table2", orders_df1.show(5))

    # +--------+----------+-----------------+---------------+
    # |order_id|order_date|order_customer_id|   order_status|
    # +--------+----------+-----------------+---------------+
    # |       1|2013-07-25|            11599|         CLOSED|
    # |       2|2013-07-25|              256|PENDING_PAYMENT|
    # |       3|2013-07-25|            12111|       COMPLETE|
    # |       4|2013-07-25|             8827|         CLOSED|
    # |       5|2013-07-25|            11318|       COMPLETE|
    # +--------+----------+-----------------+---------------+

    orders_df1.printSchema()

   # StructType
    orders_schema_struct = StructType([
        StructField("order_id", LongType(), True),
        StructField("order_date", DateType(), True),
        StructField("customer_id", LongType(), True),
        StructField("order_status", StringType(), True)])
    
    orders_df2 = spark.read.csv("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/orders.csv", schema=orders_schema_struct)
    print("table3", orders_df2.show(5))

    # +--------+----------+-----------+---------------+
    # |order_id|order_date|customer_id|   order_status|
    # +--------+----------+-----------+---------------+
    # |       1|2013-07-25|      11599|         CLOSED|
    # |       2|2013-07-25|        256|PENDING_PAYMENT|
    # |       3|2013-07-25|      12111|       COMPLETE|
    # |       4|2013-07-25|       8827|         CLOSED|
    # |       5|2013-07-25|      11318|       COMPLETE|
    # +--------+----------+-----------+---------------+

    orders_df2.printSchema()
    # root
    # |-- order_id: long (nullable = true)
    # |-- order_date: date (nullable = true)
    # |-- customer_id: long (nullable = true)
    # |-- order_status: string (nullable = true)


    #Nested Schema
    #StructType
    orders_schema_nested = StructType([
        StructField("customer_id", LongType()),
        StructField("fullname", StructType([StructField("firstname", StringType()),
                                        StructField("lastname", StringType())])),
        StructField("city", StringType())])
    
    orders_df_nested1 = spark.read.json("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/customerNested.json", schema=orders_schema_nested)

    print("table4", orders_df_nested1.show(5, truncate=False))
    # +-----------+----------------+---------+
    # |customer_id|fullname        |city     |
    # +-----------+----------------+---------+
    # |2          |{ram, kumar}    |hyderabad|
    # |3          |{vijay, shankar}|pune     |
    # |4          |{suresh, kumar} |bangalore|
    # |5          |{ravi, sharma}  |chennai  |
    # |6          |{arun, kumar}   |delhi    |
    # +-----------+----------------+---------+

    # Schema DDL
    orders_schema_ddl = 'customer_id long, fullname struct<firstname:string, lastname:string>, city string'
    orders_df_nested2 = spark.read.json("/Users/gauravmishra/Desktop/Adding/SparkDF/Datasets/customerNested.json", schema=orders_schema_ddl)

    print("table5", orders_df_nested2.show(5, truncate=False))
    # +-----------+----------------+---------+
    # |customer_id|fullname        |city     |
    # +-----------+----------------+---------+
    # |2          |{ram, kumar}    |hyderabad|
    # |3          |{vijay, shankar}|pune     |
    # |4          |{suresh, kumar} |bangalore|
    # |5          |{ravi, sharma}  |chennai  |
    # |6          |{arun, kumar}   |delhi    |
    # +-----------+----------------+---------+

    customer_list = [
        (1, ("ram", "kumar"), "hyderabad"),
        (2, ("vijay", "shankar"), "pune"),
        (3, ("suresh", "kumar"), "bangalore"),
        (4, ("ravi", "sharma"), "chennai"),
        (5, ("arun", "kumar"), "delhi")]
    
    customer_schema = 'customer_id long, fullname struct<firstname:string, lastname:string>, city string'

    customer_df = spark.createDataFrame(customer_list, schema=customer_schema)
    print("table6",customer_df.show(5))
    # +-----------+----------------+---------+
    # |customer_id|fullname        |city     |
    # +-----------+----------------+---------+
    # |1          |{ram, kumar}    |hyderabad|
    # |2          |{vijay, shankar}|pune     |
    # |3          |{suresh, kumar} |bangalore|
    # |4          |{ravi, sharma}  |chennai  |
    # |5          |{arun, kumar}   |delhi    |
    # +-----------+----------------+---------+

    customer_schema_struct = StructType([
        StructField("customer_id", LongType()),
        StructField("fullname", StructType([StructField("firstname", StringType()),
                                        StructField("lastname", StringType())])),
        StructField("city", StringType())])
    
    customer_df1 = spark.createDataFrame(customer_list, schema=customer_schema_struct)
    print("table7", customer_df1.show(5))
    # +-----------+----------------+---------+
    # |customer_id|        fullname|     city|
    # +-----------+----------------+---------+
    # |          1|    {ram, kumar}|hyderabad|
    # |          2|{vijay, shankar}|     pune|
    # |          3| {suresh, kumar}|bangalore|
    # |          4|  {ravi, sharma}|  chennai|
    # |          5|   {arun, kumar}|    delhi|
    # +-----------+----------------+---------+
    

