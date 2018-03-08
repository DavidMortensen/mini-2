from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sqlContext=SQLContext(sc)

df=sqlContext.read.format("com.databricks.spark.csv").options(header="true").load("chicago_taxi_trips_2016_01.csv")

df.show()

df.dtypes()

df.select("trip_total").show()

df.head()

df.count()

df.schema

F.sum(df.trip_total).alias("trip_total)