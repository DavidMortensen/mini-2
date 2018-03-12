from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sqlContext=SQLContext(sc)

df = spark.read.format("csv").option('header','true').load("chicago_taxi_trips_2016*.csv")

#df.show()
#df.dtypes()
df.select("trip_total").show()
#df.head()
#df.count()
#df.schema

total = dt.groupBy().agg(F.sum("trip_total")).collect()
total