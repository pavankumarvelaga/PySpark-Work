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
import numbers
def rd(a,b):
    if a is not None:
        x = a * math.pow(10,b+1)
        if x%(math.pow(10,b)) > 5:
            l = math.ceil(x/10)
        else:
            l = math.floor(x/10)
        return(float(l)/math.pow(10,b))
    else:
        return(a)
udf_rd = udf(rd,FloatType())

def int_rd(a):
    if a is not None:
        x = a * math.pow(10,1)
        if x%10 > 5:
            l = math.ceil(x/10)
        else:
            l = math.floor(x/10)
        return(int(l))
    else:
        return(a)
udf_int_rd = udf(int_rd,IntegerType())


input_cm = sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = '\t').load('gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv')
#d1 = d1.withColumn("cost",d1.reg_price*0.3)
input_cm = input_cm.filter("md_count <= 5").select("org",input_cm.h1.alias("vbs"),input_cm.h2.alias("div_no"),input_cm.h3.alias("line"),input_cm.h4.alias("item_class"),input_cm.h5.alias("item"),"store","plan_name","season_cd",(input_cm.reg_price*(1-input_cm.disc)*input_cm.ptd_ratio).alias("otd_per_unit"),"cost",input_cm.std_revenue.alias("std_dollars"),"initial_buy","on_hand",F.lower(input_cm.store_grp).alias("store_grp"),"reg_price","disc")
####################################################### Aggregation at plan level with store group ###############################################
agg_current_metrics = input_cm.groupBy("org","vbs","div_no","line","item_class","plan_name","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_plan = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("current_disc",(1-(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg)))

##################################################### Aggregation at class level with store group ####################################################


agg_current_metrics = input_cm.groupBy("org","vbs","div_no","line","item_class","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_class = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("current_disc",(1-(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg)))

################################################# Aggregation at line level with store group ###############################################################


agg_current_metrics = input_cm.groupBy("org","vbs","div_no","line","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_line = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("current_disc",(1-(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg)))

######################################### Aggregation at division level with store group ##################################################################

agg_current_metrics = input_cm.groupBy("org","vbs","div_no","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_div_no = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("current_disc",(1-(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg)))


############################################### Aggregation at VBS level with store ########################################################################

agg_current_metrics = input_cm.groupBy("org","vbs","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_vbs = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div_no",lit("ALL")).withColumn("current_disc",(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg))

################################################# Aggregation at org level with store #######################################################################

agg_current_metrics = input_cm.groupBy("org","season_cd","store_grp").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_org = agg_current_metrics.withColumn("avg_cost",(agg_current_metrics.prod_cost/agg_current_metrics.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics.unit_onhand/agg_current_metrics.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics.prod_reg/agg_current_metrics.unit_onhand)).withColumn("avg_otd",(agg_current_metrics.prod_otd/agg_current_metrics.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div_no",lit("ALL")).withColumn("vbs",lit("ALL")).withColumn("current_disc",(1-(agg_current_metrics.wtd_act/agg_current_metrics.wtd_reg)))





##################################################### Aggregation at plan level ##############################################

agg_current_metrics_no_store = input_cm.groupBy("org","vbs","div_no","line","item_class","plan_name","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_plan = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))


################################################### Aggregation at class level ###################################################

agg_current_metrics_no_store = input_cm.groupBy("org","vbs","div_no","line","item_class","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_class = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))


################################################### Aggregation at line level #######################################################

agg_current_metrics_no_store = input_cm.groupBy("org","vbs","div_no","line","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_line = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))


######################################################## Aggregation at division level #######################################################

agg_current_metrics_no_store = input_cm.groupBy("org","vbs","div_no","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_div_no = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))

################################################ Aggregation at vbs level ###################################################################

agg_current_metrics_no_store = input_cm.groupBy("org","vbs","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_vbs = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div_no",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))



################################################## Aggregation at org level ########################################################### 


agg_current_metrics_no_store = input_cm.groupBy("org","season_cd").agg(
                    F.sum(input_cm.std_dollars).alias("std_dollars"),
                                        F.sum(input_cm.cost*input_cm.on_hand).alias("prod_cost"),
                                        F.sum(input_cm.otd_per_unit*input_cm.on_hand).alias("prod_otd"),
                                        F.sum(input_cm.reg_price*input_cm.on_hand).alias("prod_reg"),
                                        F.sum(input_cm.on_hand).alias("unit_onhand"),
                                        F.sum(input_cm.initial_buy).alias("initial_buy"),
										F.sum(input_cm.reg_price * input_cm.on_hand).alias("wtd_reg"),
										F.sum(input_cm.reg_price * (1 - input_cm.disc)* input_cm.on_hand).alias("wtd_act"))

agg_current_metrics_no_store_org = agg_current_metrics_no_store.withColumn("avg_cost",(agg_current_metrics_no_store.prod_cost/agg_current_metrics_no_store.unit_onhand)).withColumn("sell_through", 1 - (agg_current_metrics_no_store.unit_onhand/agg_current_metrics_no_store.initial_buy)).withColumn("avg_reg_price",(agg_current_metrics_no_store.prod_reg/agg_current_metrics_no_store.unit_onhand)).withColumn("avg_otd",(agg_current_metrics_no_store.prod_otd/agg_current_metrics_no_store.unit_onhand)).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div_no",lit("ALL")).withColumn("vbs",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("current_disc",(1- (agg_current_metrics_no_store.wtd_act/agg_current_metrics_no_store.wtd_reg)))







agg_current_metrics_plan = agg_current_metrics_plan.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_class = agg_current_metrics_class.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_line = agg_current_metrics_line.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_div_no = agg_current_metrics_div_no.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_vbs = agg_current_metrics_vbs.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_org = agg_current_metrics_org.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")


agg_current_metrics_no_store_plan = agg_current_metrics_no_store_plan.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_no_store_class = agg_current_metrics_no_store_class.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_no_store_line = agg_current_metrics_no_store_line.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_no_store_div_no = agg_current_metrics_no_store_div_no.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_no_store_vbs = agg_current_metrics_no_store_vbs.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")

agg_current_metrics_no_store_org = agg_current_metrics_no_store_org.select("org","vbs","div_no","line","item_class","plan_name","store_grp","season_cd","std_dollars","avg_cost","avg_reg_price","avg_otd","unit_onhand","initial_buy","sell_through","current_disc")










mas1 = agg_current_metrics_plan.unionAll(agg_current_metrics_class)
mas2 = mas1.unionAll(agg_current_metrics_line)
mas3 = mas2.unionAll(agg_current_metrics_div_no)
mas4 = mas3.unionAll(agg_current_metrics_vbs)
mas5 = mas4.unionAll(agg_current_metrics_org)
mass1 = agg_current_metrics_no_store_plan.unionAll(agg_current_metrics_no_store_class)
mass2 = mass1.unionAll(agg_current_metrics_no_store_line)
mass3 = mass2.unionAll(agg_current_metrics_no_store_div_no)
mass4 = mass3.unionAll(agg_current_metrics_no_store_vbs)
mass5 = mass4.unionAll(agg_current_metrics_no_store_org)

#mass5.show(5)
final_curr_agg_data1 = mas5.unionAll(mass5)


dmap = sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = '\t').load('gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv')
#dmap1 = dmap.select("h1","h2","h3","vbs_desc","div_desc","line_desc").distinct()
dmap1_vbs = dmap.select("h1","h1_desc").distinct()
dmap1_div = dmap.select("h2","h2_desc").distinct()
dmap1_line = dmap.select("h3",dmap.h2.alias("hh2"),dmap.h1.alias("hh1"),"h3_desc").distinct()

dmap1_c = dmap.select("h4",dmap.h3.alias("hhh3"), dmap.h2.alias("hhh2"),dmap.h1.alias("hhh1"),"h4_desc").distinct().orderBy("h4_desc")
dmap1_cl = dmap1_c.groupBy("hhh1","hhh2","hhh3","h4").agg(F.collect_list(dmap1_c.h4_desc).alias("h4_ds"))
def concatinator(l):
    l = list(sorted(l))
    return('/'.join(l))
udf_concat = udf(concatinator,StringType())
dmap1_cls = dmap1_cl.select("hhh1","hhh2","hhh3","h4",udf_concat(dmap1_cl.h4_ds).alias("h4_desc"))


final_curr_agg_data1_a =  final_curr_agg_data1.join(dmap1_vbs,final_curr_agg_data1.vbs == dmap1_vbs.h1,'left')
final_curr_agg_data1_b =  final_curr_agg_data1_a.join(dmap1_div,final_curr_agg_data1_a.div_no == dmap1_div.h2,'left')
final_curr_agg_data1_c =  final_curr_agg_data1_b.join(dmap1_line,[(final_curr_agg_data1_b.line == dmap1_line.h3) & (final_curr_agg_data1_b.vbs == dmap1_line.hh1) & (final_curr_agg_data1_b.div_no == dmap1_line.hh2)],'left')
final_curr_agg_data1_x =  final_curr_agg_data1_c.join(dmap1_cls,[(final_curr_agg_data1_c.item_class == dmap1_cls.h4) & (final_curr_agg_data1_c.line == dmap1_cls.hhh3) & (final_curr_agg_data1_c.vbs == dmap1_cls.hhh1) & (final_curr_agg_data1_c.div_no == dmap1_cls.hhh2)],'left')

cadence1 = HiveContext.sql("select plan_name,mdt1,mdt2,mdt3,mdt4,mdt5,mos,md_count from final_whatif_table_md_level_new")
cadence1 = (cadence1.withColumn("md1_length_d",F.when(cadence1.md_count < 1,'-').otherwise(F.when(cadence1.mdt2 == 1000,(cadence1.mos - cadence1.mdt1)).otherwise(cadence1.mdt2-cadence1.mdt1))).withColumn("md2_length_d",F.when(cadence1.md_count < 2,'-').otherwise(F.when(cadence1.mdt3 == 1000,(cadence1.mos - cadence1.mdt2)).otherwise(cadence1.mdt3-cadence1.mdt2))).withColumn("md3_length_d",F.when(cadence1.md_count < 3,'-').otherwise(F.when(cadence1.mdt4 == 1000,(cadence1.mos - cadence1.mdt3)).otherwise(cadence1.mdt4-cadence1.mdt3))).withColumn("md4_length_d",F.when(cadence1.md_count < 4,'-').otherwise(F.when(cadence1.mdt5 == 1000,(cadence1.mos - cadence1.mdt4)).otherwise(cadence1.mdt5-cadence1.mdt4))).withColumn("md5_length_d",F.when(cadence1.md_count < 5,'-').otherwise(cadence1.mos-cadence1.mdt5)).withColumn("num_mds",F.when(cadence1.md_count > 5 , 5).otherwise(cadence1.md_count))
)

def intifier(x):
    try:
        return(int(x))
    except:
        return(x)
udf_int = udf(intifier,IntegerType())

cadence2 = (cadence1.select(F.when(cadence1.num_mds == 1,udf_int(cadence1.md1_length_d + int(1))).otherwise(cadence1.md1_length_d).alias("md1_length"),F.when(cadence1.num_mds == 2,udf_int(cadence1.md2_length_d + int(1))).otherwise(cadence1.md2_length_d).alias("md2_length"),
F.when(cadence1.num_mds == 3,udf_int(cadence1.md3_length_d + int(1))).otherwise(cadence1.md3_length_d).alias("md3_length"),F.when(cadence1.num_mds == 4,udf_int(cadence1.md4_length_d + int(1))).otherwise(cadence1.md4_length_d).alias("md4_length"),
F.when(cadence1.num_mds == 5,udf_int(cadence1.md5_length_d + int(1))).otherwise(cadence1.md5_length_d).alias("md5_length"),"num_mds","plan_name"))
#cadencez = cadencey.withColumn()

cadence_mapper =  cadence2.select(cadence2.plan_name.alias("plan_name1"),"md1_length","md2_length","md3_length","md4_length","md5_length","num_mds").distinct()


current_cadence_metrics = final_curr_agg_data1_x.join(broadcast(cadence_mapper),final_curr_agg_data1_x.plan_name == cadence_mapper.plan_name1,'left')

comp1_data = current_cadence_metrics.filter("org like 'S%'")
comp2_data = current_cadence_metrics.filter("org like 'K%'")

s = comp1_data.count()
k = comp2_data.count()


comp1_data_f = comp1_data.select("org",F.when(comp1_data.vbs == 'ALL','ALL').otherwise(concat(comp1_data.vbs,lit(" - "),coalesce(comp1_data.h1_desc,lit("")))).alias("business"),F.when(comp1_data.div_no == 'ALL','ALL').otherwise(concat(comp1_data.div_no,lit(" - "),coalesce(comp1_data.h2_desc,lit("")))).alias("division"),F.when(comp1_data.line == 'ALL','ALL').otherwise(concat(comp1_data.line,lit(" - "),coalesce(comp1_data.h3_desc,lit("")))).alias("line_cat"), F.when(comp1_data.item_class == 'ALL','ALL').otherwise(concat(comp1_data.item_class,lit(" - "),coalesce(comp1_data.h4_desc,lit("")))).alias("cls_subcat"),comp1_data.season_cd.alias("season"),upper(comp1_data.store_grp).alias("store_grp"),comp1_data.plan_name.alias("plan_id"),udf_rd(comp1_data.avg_reg_price,lit(2)).alias("dlr_avg_reg_price"),udf_int_rd(comp1_data.avg_otd).alias("dlr_otd"),udf_int_rd(comp1_data.current_disc * 100).alias("current_disc"),udf_int_rd(comp1_data.avg_cost).alias("dlr_avg_cost"),udf_int_rd(comp1_data.sell_through * 100).alias("perc_sellthrough"),"unit_onhand",udf_int_rd(comp1_data.std_dollars).alias("dlr_rev"),coalesce(comp1_data.md1_length,lit('-')).alias("md1_length"),coalesce(comp1_data.md2_length,lit('-')).alias("md2_length"),coalesce(comp1_data.md3_length,lit('-')).alias("md3_length"),coalesce(comp1_data.md4_length,lit('-')).alias("md4_length"),coalesce(comp1_data.md5_length,lit('-')).alias("md5_length"),coalesce(comp1_data.num_mds,lit('-')).alias("num_mds"))

comp2_data_f = comp2_data.select("org",F.when(comp2_data.vbs == 'ALL','ALL').otherwise(concat(comp2_data.vbs,lit(" - "),coalesce(comp2_data.h1_desc,lit("")))).alias("business"),comp2_data.div_no.alias("division"),F.when(comp2_data.line == 'ALL','ALL').otherwise(concat(comp2_data.line,lit(" - "),coalesce(comp2_data.h3_desc,lit("")))).alias("line_cat"),F.when(comp2_data.item_class == 'ALL','ALL').otherwise(concat(comp2_data.item_class,lit(" - "),coalesce(comp2_data.h4_desc,lit("")))).alias("cls_subcat"),comp2_data.season_cd.alias("season"),upper(comp2_data.store_grp).alias("store_grp"),comp2_data.plan_name.alias("plan_id"),udf_rd(comp2_data.avg_reg_price,lit(2)).alias("dlr_avg_reg_price"),udf_int_rd(comp2_data.avg_otd).alias("dlr_otd"),udf_int_rd(comp2_data.current_disc * 100).alias("current_disc"),udf_int_rd(comp2_data.avg_cost).alias("dlr_avg_cost"),udf_int_rd(comp2_data.sell_through * 100).alias("perc_sellthrough"),"unit_onhand",udf_int_rd(comp2_data.std_dollars).alias("dlr_rev"),coalesce(comp2_data.md1_length,lit('-')).alias("md1_length"),coalesce(comp2_data.md2_length,lit('-')).alias("md2_length"),coalesce(comp2_data.md3_length,lit('-')).alias("md3_length"),coalesce(comp2_data.md4_length,lit('-')).alias("md4_length"),coalesce(comp2_data.md5_length,lit('-')).alias("md5_length"),coalesce(comp2_data.num_mds,lit('-')).alias("num_mds"))

final_curr_agg_data = comp1_data_f.unionAll(comp2_data_f)

dummy_md= sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = ',').load('gs://affine_poc/skprccm/dummy_data/current_metrics_dummy.csv')

comp2_dummy_cm = dummy_md.filter("a like 'K%'")
comp1_dummy_cm = dummy_md.filter("a like 'S%'")

if s == 0:
    final_curr_agg_data_1 = final_curr_agg_data.unionAll(comp1_dummy_cm)
elif k == 0:
    final_curr_agg_data_1 = final_curr_agg_data.unionAll(comp2_dummy_cm)
else:
    final_curr_agg_data_1 = final_curr_agg_data

final_curr_agg_data_1.write.saveAsTable('current_metrics_final_x')

