from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName("Fundamentals").getOrCreate()

# Read data with spark

file_path = "/src/files/test1.csv"

#df_spark = spark.read.csv(file_path, header = True)

df_spark = spark.read.option("header", "true").csv(file_path)

# Show spark dataframe

df_spark.show()

type(df_spark)

df_spark.printSchema()

# Data preprocessing

df_spark.columns

df_spark.head(4) # get result as a list format

df_spark.select('Name') # get a column name 'Name', slicing is not working for spark dataframe

df_spark.select(['Name', 'Experience'])

df_spark.dtypes

df_spark.describe()

df_spark.withColumn('New Experience Column', df_spark['Experience']) # adding columns

df_spark.drop('New Experience Column') # drop columns

df_spark.withColumnRenamed('Name', 'New Name') # rename the columns

# Drop na records

df_spark.na.drop() # drop null records

df_spark.na.drop(how='all') # there are two how options, any and all. all means that if row have any not null values, it won't drop. any drop all record if there is any null value.

df_spark.na.drop(how='any', thresh=2) # threshold=2 means that at least two not null values should be present. if row have one null value and two not null value, it won't drop

df_spark.na.drop(how='any', subset=['Experience']) # drop specific column's null value

df_spark.na.fill('Missing Values', 'Experience') # fill experience column's null values as a Missing Values

df_spark.na.fill('Missing Values', ['Experience', 'Name']) # fill experience and age column's null values as a Missing Values

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'],
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")

imputer.fit(df_spark).transform(df_spark).show() # to fill null values as a median with using imputer function

# Filter function

df_spark.filter("Salary <= 2000") # how to use filter function on specific column

df_spark.filter("Salary <= 2000").select(["Name", "Age"]) # filter column and get specific columns

df_spark.filter((df_spark['Salary'] <= 20000) & (df_spark['Salary'] >= 15000))

df_spark.filter(~(df_spark['Salary'] <= 20000))

# GroupBy function

df_spark.groupBy('Name').sum() # group values using column and use aggregate function

df_spark.groupBy('Name').avg()

df_spark.groupBy('Departments').count()

df_spark.groupBy('Departments').mean()

#Aggregate Function

df_spark.agg({'Salary':'sum'})

df_spark.agg({'Salary':'avg'})




