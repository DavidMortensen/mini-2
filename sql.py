from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sqlContext=SQLContext(sc)
#loads in all files
df = spark.read.format("csv").option('header','true').load("../chicago-taxi-rides-2016/chicago_taxi_trips_2016_*.csv")

#df.show()
#df.dtypes()
#selects the column trip_total
df.select("trip_total").show()
#df.head()
#df.count()
#df.schema

#sum of column "trip_total"
total = dt.groupBy().agg(F.sum("trip_total")).collect()
total