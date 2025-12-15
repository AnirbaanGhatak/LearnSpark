from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import codecs

# spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# people = spark.read.option("header", "true").option("inferSchema", "true")\
#     .csv("./resources/datasets/fakefriends-header.csv")
    
# print("Here is our inferred schema:")
# people.printSchema()

# print("Let's display the name column:")
# people.select("name").show()

# print("Filter out anyone over 21:")
# people.filter(people.age < 21).show()

# print("Group by age")
# people.groupBy("age").count().show()

# print("Make everyone 10 years older:")
# people.select(people.name, people.age + 10).show()

# spark.stop()

# spark = SparkSession.builder.appName("SQL Exercise").getOrCreate()

# ffriends = spark.read.option("header", "true").option("inferSchema", "true").csv("./resources/datasets/fakefriends-header.csv")

# ffriends.printSchema()

# print("Average Age")

# ffriends_lean = ffriends.select("age", "friends")

# # ffriends_lean.groupBy("age").avg("friends").sort("age").show()

# ffriends_lean.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("Average_Friends")).sort("age").show()

# spark.stop()


# spark = SparkSession.builder.appName("SQL Exercise 2").getOrCreate()

# schema = StructType([\
#                       StructField("cID", IntegerType(), True),
#                       StructField("prodID", IntegerType(), True),
#                       StructField("cost", FloatType(), True)    
#     ])

# cust_data = spark.read.schema(schema=schema).csv("./resources/datasets/customer-orders.csv")

# c_data = cust_data.select("cID", "cost")

# sum_data = c_data.groupBy("cID").agg(func.round(func.sum("cost"), 2).alias("Total Customer Spend"))
# sum_data = sum_data.sort("Total Customer Spend")

# sum_data.show(sum_data.count())

# spark.stop()

def loadMovieNames():
    moviesNames = {}
    
    with codecs.open("./resources/datasets/ml-100k/u.item", "r", encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            moviesNames[int(fields[0])] = fields[1]

            print(moviesNames)
        return moviesNames

spark = SparkSession.builder.appName("Popular Movies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True),
])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./resources/datasets/ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

sortedMoviwswithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviwswithNames.show(10, False)

spark.stop()