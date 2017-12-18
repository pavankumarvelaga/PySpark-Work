#Importing necessary libraries
import math
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import numpy as np
import re as re
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import  SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkContext
import os 

sc = SparkContext()
HiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)

#Importing input tables
dx= sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = '\t').load('gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv')
part_counter = 1

#Filtering data
d1 = dx.filter(dx.md_count < 4)
d1 = d1.withColumn('part',lit(1))
w = Window().partitionBy("part").orderBy('key')
d1 = d1.withColumn('row_number',F.row_number().over(w))

a = 0
x = 500000
b = 500000
c = d1.count()

#Batching till we run out of data
while a<c:
    if(a+x >= c):
        b= b+1
    file_name = "part_one_" + str(part_counter)
    part_counter = part_counter + 1
    temp_data = d1.filter((d1.row_number >= a) & (d1.row_number < b))
    a= a + x
    b = b + x
    temp_data.write.saveAsTable(file_name)
	
	
d2 = dx.filter(dx.md_count == 4)
d2 = d2.withColumn('part',lit(1))
w = Window().partitionBy("part").orderBy('key')
d2 = d2.withColumn('row_number',F.row_number().over(w))
a = 0
x = 100000
b = 100000
c = d2.count()
while a<c:
    if(a+x >= c):
        b= b+1
    file_name = "part_one_" + str(part_counter)
    part_counter = part_counter + 1
    temp_data = d2.filter((d2.row_number >= a) & (d2.row_number < b))
    a= a + x
    b = b + x
    temp_data.write.saveAsTable(file_name)	
	
	
d3 = dx.filter(dx.md_count == 5)
d3 = d3.withColumn('part',lit(1))
w = Window().partitionBy("part").orderBy('key')
d3 = d3.withColumn('row_number',F.row_number().over(w))
a = 0
x = 50000
b = 50000
c = d3.count()
while a<c:
    if(a+x >= c):
        b= b+1
    file_name = "part_one_" + str(part_counter)
    part_counter = part_counter + 1
    temp_data = d3.filter((d3.row_number >= a) & (d3.row_number < b))
    a= a + x
    b = b + x
    temp_data.write.saveAsTable(file_name)