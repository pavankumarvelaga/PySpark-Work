#Importing necessary librarirs
import math
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import numpy as np
import re as re
from pyspark.sql.types import *
from pyspark.sql import  SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkContext
sc = SparkContext()
HiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)

#Creating Data Frame with all possible scenario Combinations
final_df_all3 = HiveContext.sql("select * from final_one_1")
mapper = sc.parallelize([[0,0,0,0,0,1],[1,-5,1,0,0,0],[1,-5,1,0,1,0],[1,-10,1,0,0,0],[1,-10,1,0,1,0],[1,-15,1,0,0,0],[1,-15,1,0,1,0],[2,-5,0,1,0,0],[2,-5,0,1,1,0],[2,-10,0,1,0,0],[2,-10,0,1,1,0],[2,-15,0,1,0,0],[2,-15,0,1,1,0],[3,-5,1,0,1,0],[3,-5,0,0,1,0],[3,-10,1,0,1,0],[3,-10,0,0,1,0],[3,-15,1,0,1,0],[3,-15,0,0,1,0],[3,-15,0,1,1,0],[3,-10,0,1,1,0],[3,-5,0,1,1,0],[1,5,1,0,0,0],[1,5,1,0,1,0],[1,10,1,0,0,0],[1,10,1,0,1,0],[1,15,1,0,0,0],[1,15,1,0,1,0],[2,5,0,1,0,0],[2,5,0,1,1,0],[2,10,0,1,0,0],[2,10,0,1,1,0],[2,15,0,1,0,0],[2,15,0,1,1,0],[3,5,1,0,1,0],[3,5,0,0,1,0],[3,10,1,0,1,0],[3,10,0,0,1,0],[3,15,1,0,1,0],[3,15,0,0,1,0],[3,15,0,1,1,0],[3,10,0,1,1,0],[3,5,0,1,1,0]])

schema_mapper=StructType([
        StructField("scenario",StringType(), True),
        StructField("md_Depth",IntegerType(), True),
        StructField("sin1",IntegerType(), True),
        StructField("sin2",IntegerType(), True),
        StructField("sin3",IntegerType(), True),
        StructField("maxcomb1",IntegerType(), True)
         ,])
df_all_scene_x  = HiveContext.createDataFrame(mapper, schema_mapper)


# Selecting Distinct keys from data frame
keyall  = final_df_all3.select(final_df_all3.key.alias("key2")).distinct()


#Cross Joining Keys with all possible combinations to create base table
key_mapper = keyall.join(broadcast(df_all_scene_x))

#Joining Base Table with Metrics table(output of metric_caluculator)
initial_merge = key_mapper.join(final_df_all3, [(key_mapper.key2 == final_df_all3.key) & (key_mapper.md_Depth == final_df_all3.md1_depth) & (key_mapper.sin1 == final_df_all3.scenario1) & (key_mapper.sin2 == final_df_all3.scenario2) & (key_mapper.sin3 == final_df_all3.scenario3) & (key_mapper.maxcomb1 == final_df_all3.maxcomb) ],"left")
ww = Window.partitionBy("key2","md_depth","scenario")
initial_merge = initial_merge.withColumn("counter",count(initial_merge.key).over(ww))


# Imputing Rows with optimal scenario
opt_merge = initial_merge.filter("(key is null) and ((sin1+sin2+sin3) <= 1) and counter < 1").select("key2","scenario","md_Depth","sin1","sin2","sin3","maxcomb1").repartition("key2")
initial_merge = initial_merge.drop("counter")

final_df_all3 = final_df_all3.repartition("key")




null_replaced_opt = opt_merge.join(final_df_all3,[(opt_merge.key2 == final_df_all3.key) & (final_df_all3.maxcomb == 1)],"left")


scenario_table = initial_merge.filter("key is not null")

# Union Both Imputed as well as actual scenario metrics
final_merge = scenario_table.unionAll(null_replaced_opt)
final_merge.write.saveAsTable('final_whatif_table_md_level_new')

