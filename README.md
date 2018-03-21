# How to guide for Mini Project 2, group 9.
## SQL.py
We used a python syntax so the commenting fits the same style throughout. The commands are to be copied into spark shell.
The file locations can be changed in the initial file load, currently it's based on our dir setup

## RDD.py
Once again we used a python syntax because of commenting.
As with SQL, you are to just copy the contents from the file into spark shell.

## chicago.py
This is our python standalone script with cached files. It's run through terminal with the following arguments:
Argument 1: Trip data directory you want to use. 
Argument 2: The location of our drivers data. This have to be file specific, and can't be a directory
Argument 3: The query you want to answer. You can type query1, query2, query3 and query4.

Example: python chicago_no_caching.py ../12 ../chicago_taxi_drivers.csv query3 

## chicago_no_caching.py
Identical to chicago.py, but in this script we load the files on query instead of initially. The files will not be cached in memory.

