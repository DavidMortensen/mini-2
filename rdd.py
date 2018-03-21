from pyspark import SparkContext, SparkConf
import csv
sc = SparkContext (master = 'local[1]')
# define which directory we want to load our rides from
data_temp = sc.textFile("../chicago-taxi-rides-2016")
# define which file we want to load or drivers from
taxi_drivers = sc.textFile("../chicago_taxi_drivers.csv")
taxi_drivers = taxi_drivers.map(lambda line: line.split(','))

#Removing header line
data = data_temp.map(lambda line: line.split(','))
header = data.first()
data = data.filter(lambda row: row != header)

#OPTIONAL: Caching 
data.cache()
taxi_drivers.cache()

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
# we index on column 13 where we have the value trip_total
total_sum = data.map(lambda x: x[13]).filter(lambda x: len(x) != 0)
total_sum = total_sum.map(lambda x: float(x))
print(total_sum.sum())

#query 2 - sum company turnover
# we first filter our data to remove all 0 values from both company and trip total
data_filtered = data.filter(lambda x: len(x[15]) != 0)
data_filtered = data_filtered.filter(lambda x: len(x[13]) != 0)
# assign amount paid from column 13 and convert to float throughout
amount = data_filtered.map(lambda x: x[13]).map(lambda x: float(x))
company = data_filtered.map(lambda x: x[15])
# create tuple with company and amount in each tuple
joined = company.zip(amount)
# group by company so we sum the amount paid in each company respectively
y = joined.reduceByKey(lambda x,y: x+y)
y.take(10)

#query 3 - sum cash trips:
cash = data.filter(lambda x: x[14] == 'Cash')
# remove all rows that have 0 in our trip_total column
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

names = joined.map(lambda x: x[1][1])
names.collect()

#query 4 other way

taxi_id = data.filter(lambda x: x[15] == '11').map(lambda x: x[0])
taxi_comp = data.filter(lambda x: x[15] == '11').map(lambda x: x[15])
id_n_comp = taxi_id.zip(taxi_comp)
joined = taxi_drivers.join(id_n_comp)
names = joined.map(lambda x: x[1][0]).distinct()
names.collect()