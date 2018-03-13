""" Initialize all files"""
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sqlContext=SQLContext(sc)
#loads in all files
df = spark.read.format("csv").option('header','true').load("../chicago-taxi-rides-2016/chicago_taxi_trips_2016_*.csv")
df_drivers = spark.read.format("csv").option('header','true').load("../chicago_taxi_drivers.csv") 

#df.show()
#df.dtypes()
#selects the column trip_total
df.select("trip_total").show()
#df.head()
#df.count()
#df.schema

"""Query 1 """
#sum of column "trip_total"
dt = df
total = dt.groupBy().agg(F.sum("trip_total")).collect()
total

"""Query 2"""
company_total_df = df.withColumn("trip_total",df["trip_total"].cast('float'))
 #groups by company and sums the value trip_total in the new column individual_trip_total
company_total_df.groupBy("company").agg(F.sum("trip_total").alias("individual_trip_total")).show()  

"""Query 3"""
payment_type_df = df.withColumn("trip_total",df["trip_total"].cast('float'))
 #groups by company and sums the value trip_total in the new column individual_trip_total
payment_type_df.groupBy("payment_type").agg(F.sum("trip_total").alias("payment_type_total")).show() 



"""Query 4"""
#reads in the driver file
drivers=spark.read.option('header','false').csv("chicago_taxi_drivers.csv")
#changing column name
drivers=drivers.withColumnRenamed("_c0", "taxi_id")
#joining driver file with dataframe
total_df = dt.join(drivers, on=['taxi_id'], how = 'left_outer')
total_df=join.withColumnRenamed("_c1", "driver_name")

company_11=spark.sql("SELECT driver_name, company FROM total_df WHERE company='11'")
company_11.show()
company_11.count() #14 drivers from company 11





