#importing all necessary Libraries
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
#sqlContext = sqlc
sqlContext = SQLContext(sc)



# Function to to append identifiers to week row
def row_indentifier_try(key,plan_name,scenario,md_depth,lst,store_grp,org,season_cd,ib):
    a = lst
    b = list(map(lambda x : x.extend([key,plan_name,scenario,md_depth,store_grp,org,season_cd,ib]),a))

    return(a)

row_indentifier_udf1 = udf(row_indentifier_try, ArrayType(ArrayType(StringType())))  


# Reading Data from hive table
df0 = HiveContext.sql("select * from final_whatif_table_md_level_new")
df0 = df0.withColumn("new_week_dat_try",row_indentifier_udf1(df0.key, df0.plan_name, df0.scenario, df0.md_Depth, df0.weeklevel_data,df0.store_grp,df0.org,df0.season_cd,df0.initial_buy))
d1 = df0.select("new_week_dat_try").rdd.flatMap(lambda x : x).flatMap(lambda x:x)

# Creating Data Frame from multiple lists
schema2=StructType([
        StructField("week",StringType(), True),
        StructField("wk_revenue",StringType(), True),
        StructField("margin_dlr",StringType(), True),
        StructField("cmd_dlr",StringType(), True),
        StructField("current_OH",StringType(), True),
		StructField("perc_mrgn",StringType(), True),
		StructField("cr_cost",StringType(), True),
        StructField("units_sold",StringType(), True),
		StructField("key",StringType(), True),
		StructField("plan_name",StringType(), True),
        StructField("scenario",StringType(), True),
		StructField("md_depth",StringType(), True),
		StructField("store_grp",StringType(), True),
        StructField("org",StringType(), True),
        StructField("season_cd",StringType(), True),
		StructField("ib",StringType(), True),
    ])
	
final_merge_week = HiveContext.createDataFrame(d1, schema2)
final_merge_week.write.saveAsTable('week_level_extracted_table')
