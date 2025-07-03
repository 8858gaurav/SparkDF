import pyspark
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
import getpass
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
            .config("spark.ui.port","4040") \
            .master("local[*]") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # creating a df using RDD

    rdd = spark.sparkContext.parallelize(range(1, 10))
    df = rdd.map(lambda x: (x, x * 2)).toDF(["id", "value"])
    df.show()

    rdd1 = spark.sparkContext.parallelize(range(1, 10))
    df1 = rdd1.map(lambda x: (x, "row: {}".format(x))).toDF(["id",  "value"])
    df1.show()

    df1.foreach(lambda x: print(x))

    df1.collect()
    
    

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
    print("head 5: ", df3.head(5))
    print("take 5: ", df3.take(5))
    print("columns: ", df3.columns)
    print("dtypes: ", df3.dtypes)
    print("Number of rows in df3: ", df3.count())
    print("Number of columns in df3: ", len(df3.columns))
    df3.printSchema()
    

    # how to print few lines with foreach function
#     df3.take(3).foreach(lambda x: print(x)) # this will not work, as take returns a list
    df3.foreach(lambda x: print(x)) # this will work, as foreach is an action and will print each row