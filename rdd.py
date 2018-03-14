from pyspark import SparkContext, SparkConf
import csv
sc = SparkContext (master = 'local[1]')

data_temp = sc.textFile("../chicago-taxi-rides-2016")

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

#query 2 - sum company turnover
data_filtered = data.filter(lambda x: len(x[15]) != 0)
data_filtered = data_filtered.filter(lambda x: len(x[13]) != 0)
amount = data_filtered.map(lambda x: x[13]).map(lambda x: float(x))
company = data_filtered.map(lambda x: x[15])
joined = company.zip(amount)
y = joined.reduceByKey(lambda x,y: x+y)
y.take(10)

#query 3 - sum cash trips:
cash = data.filter(lambda x: x[14] == 'Cash')
cash_payments = cash.map(lambda x: x[13]).filter(lambda x: len(x) != 0)
cash_payments = cash_payments.map(lambda x: float(x))
print(cash_payments.sum())

#query 4
data_filtered = data.filter(lambda x: len(x[0]) != 0)
data_filtered = data_filtered.filter(lambda x: len(x[15]) != 0)
taxi_id = data_filtered.map(lambda x: x[0])
company = data_filtered.map(lambda x: x[15])
company = taxi_id.zip(company)
company = company.filter(lambda x: x[1] == '11')

company = company.distinct()
joined = company.join(taxi_id)

if not joined.isEmpty():
    joined = joined.collect()
    for item in joined:
        print(item)
