from pyspark import SparkContext, SparkConf
import csv
sc = SparkContext (master = 'local[2]')

data_temp = sc.textFile("chicago_taxi_trips_2016_01.csv")

#Removing header line
data = data_temp.map(lambda line: line.split(','))
header = data.first()
data = data.filter(lambda row: row != header)

"""
Index values: 
  'taxi_id', = 0
  'trip_start_timestamp' = 1
  'trip_end_timestamp' = 2
  'trip_seconds' = 3
  'trip_miles' = 4
  'pickup_census_tract' = 5
  'dropoff_census_tract' = 6
  'pickup_community_area' = 7
  'dropoff_community_area'= 8
  'fare'= 9
  'tips' = 10
  'tolls' = 11
  'extras' = 12
  'trip_total' = 13
  'payment_type' = 14
  'company' = 15
  'pickup_latitude' =16
  'pickup_longitude' = 17
  'dropoff_latitude' = 18
  'dropoff_longitude'] = 19
  """
  
#query 1 - sum trips:
total_sum = data.map(lambda x: x[13]).filter(lambda x: len(x) != 0)
total_sum = total_sum.map(lambda x: float(x))
print(total_sum.sum())


#query 3 - sum cash trips:
cash = data.map(lambda x: x[14]).filter(lambda x: x == 'Cash')
cash_payments = cash.map(lambda x: x[13]).filter(lambda x: len(x) != 0)
cash_total = cash_payments.map(lambda x: float(x))
print(cash_tot.sum())