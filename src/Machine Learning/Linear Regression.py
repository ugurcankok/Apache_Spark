from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark=SparkSession.builder.appName('Linear Regression').getOrCreate()

training = spark.read.csv('C:/Users/8130/Desktop/ugurcan/Apache Spark/src/files/test1.csv',header=True, inferSchema=True)

#training.show()

featureassembler = VectorAssembler(inputCols=["age","Experience"],outputCol="Independent Features")

output = featureassembler.transform(training)

#output.show()

finalized_data = output.select("Independent Features","Salary")

train_data,test_data = finalized_data.randomSplit([0.75,0.25])

regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')

regressor = regressor.fit(train_data)

pred_results = regressor.evaluate(test_data)

pred_results.predictions.show()

print("Mean Absolute Error: ", pred_results.meanAbsoluteError)

print("Mean Square Error: ", pred_results.meanSquaredError)