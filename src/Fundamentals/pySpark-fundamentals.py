from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Fundamentals").getOrCreate()

file_path = "/src/files/test1.csv"

#df_spark = spark.read.csv(file_path, header = True)

df_spark = spark.read.option("header", "true").csv(file_path)

df_spark.show()

type(df_spark)

df_spark.printSchema()

# Data preprocessing

df_spark.columns

df_spark.head(4) # get result as a list format

df_spark.select('Name') # get a column 'Name', slicing is not working for spark dataframe

df_spark.select(['Name', 'Experience'])

df_spark.dtypes

df_spark.describe()

df_spark.withColumn('New Experience Column', df_spark['Experience']) # adding columns

df_spark.drop('New Experience Column') # drop columns

df_spark.withColumnRenamed('Name', 'New Name') # rename the columns









