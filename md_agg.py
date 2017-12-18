# importing all necessary Libraries 
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



# Function to split string
def splitstring(a):
    lm = [int(i) for i in a.split("-")]
    return(lm)

udf1 = udf(splitstring,ArrayType(IntegerType()))

# Reading Data from Hive
final_merge1 =  HiveContext.sql("select * from final_whatif_table_md_level_new")
final_merge2  = final_merge1.withColumn("store_grp_1",F.lower(final_merge1.store_grp))

final_merge = final_merge2.withColumn("VBS",udf1(final_merge2.key)[0]).withColumn("DIV",udf1(final_merge2.key)[1]).withColumn("LINE",udf1(final_merge2.key)[2]).withColumn("item_class",udf1(final_merge2.key)[3]).withColumn("ITEM",udf1(final_merge2.key)[4]).withColumn("STORE",udf1(final_merge2.key)[5])

final_merge = final_merge.withColumn("total_units",(final_merge.md1_units + final_merge.md2_units + final_merge.md3_units +  final_merge.md4_units + final_merge.md5_units))\
                        .withColumn("total_cmd",(final_merge.md1_cmd + final_merge.md2_cmd + final_merge.md3_cmd +  final_merge.md4_cmd + final_merge.md5_cmd))

final_merge = final_merge.withColumn("avg_units_sld" ,(final_merge.total_units/(final_merge.mos- final_merge.mdt1 +1)))

# Function to Round as float values
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

# Function to Round as Integer values
def int_rd(a):
    if a is not None:
        x = a * math.pow(10,1)
        if x%10 > 5:
            l = math.ceil(x/10)
        else:
            l = math.floor(x/10)
        return(int(l)/1)
    else:
        return(a)
udf_int_rd = udf(int_rd,IntegerType())

#Plan level aggregation
############################################ Aggregation at plan and Store group Level #################################################
all_store_agg = final_merge.groupBy("org","VBS","DIV","LINE","item_class","season_cd","plan_name","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
			) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
all_store_agg_pln = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                              .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                              .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100)

all_store_agg_pln = all_store_agg_pln.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")

###################### Aggregation at Class and store group level ################											  
all_store_agg = final_merge.groupBy("org","VBS","DIV","LINE","item_class","season_cd","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
	
all_store_agg_class = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                              .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                              .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100)  .withColumn("plan_name",lit("ALL"))											  
											  


all_store_agg_class = all_store_agg_class.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")
											  
											  
											  
											  
											  
											  
											  
################################ Aggregation at line and store group level ################################											  
all_store_agg = final_merge.groupBy("org","VBS","DIV","LINE","season_cd","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
			) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
	
all_store_agg_line = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL"))


all_store_agg_line = all_store_agg_line.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")

											 
################################### Aggregation at division and store group level ################################											 
all_store_agg = final_merge.groupBy("org","VBS","DIV","season_cd","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
	
all_store_agg_div = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL"))
											 
all_store_agg_div = all_store_agg_div.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")											 

################################# Aggregation at VBS and store group level #######################################											 
all_store_agg = final_merge.groupBy("org","VBS","season_cd","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
	
all_store_agg_vbs = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL"))\
                                             .withColumn("DIV",lit("ALL"))\

											 
all_store_agg_vbs = all_store_agg_vbs.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")											 

############################# Aggregation at org and store group level ############################### 											 
all_store_agg = final_merge.groupBy("org","season_cd","store_grp_1","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
) 
    
    
    
    
all_store_agg = all_store_agg.withColumn("final_gm",((all_store_agg.net_rev-all_store_agg.tot_cost)/ \
                      all_store_agg.net_rev))\
    .withColumn("final_st",(all_store_agg.initial_buy-all_store_agg.final_oh)/all_store_agg.initial_buy)\
    .withColumn("md1_gm",((all_store_agg.md1_rev-all_store_agg.md1_cost)/all_store_agg.md1_rev))\
    .withColumn("md2_gm",F.when(all_store_agg.md2_rev>0,(all_store_agg.md2_rev-all_store_agg.md2_cost)/all_store_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(all_store_agg.md3_rev>0,(all_store_agg.md3_rev-all_store_agg.md3_cost)/all_store_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(all_store_agg.md4_rev>0,(all_store_agg.md4_rev-all_store_agg.md4_cost)/all_store_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(all_store_agg.md5_rev>0,(all_store_agg.md5_rev-all_store_agg.md5_cost)/all_store_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(all_store_agg.initial_buy-all_store_agg.md1_oh)/all_store_agg.initial_buy)\
    .withColumn("md2_st",(all_store_agg.initial_buy-all_store_agg.md2_oh)/all_store_agg.initial_buy)\
    .withColumn("md3_st",(all_store_agg.initial_buy-all_store_agg.md3_oh)/all_store_agg.initial_buy)\
    .withColumn("md4_st",(all_store_agg.initial_buy-all_store_agg.md4_oh)/all_store_agg.initial_buy)\
    .withColumn("md5_st",(all_store_agg.initial_buy-all_store_agg.md5_oh)/all_store_agg.initial_buy)\
    .withColumn("weighted_reg",(all_store_agg.prod_reg_price)/all_store_agg.initial_buy)\
    .withColumn("weighted_md1_price",(all_store_agg.md1_crp_prod)/all_store_agg.md1_units)\
    .withColumn("weighted_md2_price",(all_store_agg.md2_crp_prod)/all_store_agg.md2_units)\
    .withColumn("weighted_md3_price",(all_store_agg.md3_crp_prod)/all_store_agg.md3_units)\
    .withColumn("weighted_md4_price",(all_store_agg.md4_crp_prod)/all_store_agg.md4_units)\
    .withColumn("weighted_md5_price",(all_store_agg.md5_crp_prod)/all_store_agg.md5_units)\
	.withColumn("overall_md_crp",((all_store_agg.md1_crp_prod+all_store_agg.md2_crp_prod+all_store_agg.md3_crp_prod+all_store_agg.md4_crp_prod+all_store_agg.md5_crp_prod)/all_store_agg.total_units))
    
	
all_store_agg_all = all_store_agg.withColumn("md1", (1-(all_store_agg.weighted_md1_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(all_store_agg.weighted_md2_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(all_store_agg.weighted_md3_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(all_store_agg.weighted_md4_price/all_store_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(all_store_agg.weighted_md5_price/all_store_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(all_store_agg.overall_md_crp/all_store_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL")).withColumn("DIV",lit("ALL")).withColumn("VBS",lit("ALL"))												 
											 
all_store_agg_all = all_store_agg_all.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")
											 
											 

											 
											 
											 
											 
#All aggregations with store group as all. 

#################################### Aggregation at Plan level  #########################################
store_grp_all_agg = final_merge.groupBy("org","VBS","DIV","LINE","item_class","season_cd","plan_name","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
	.withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
	
store_grp_all_agg_pln = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                              .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                              .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100).withColumn("store_grp_1",lit("ALL"))
											 
store_grp_all_agg_pln = store_grp_all_agg_pln.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")

############################################# Aggregation at Class Level ########################################											  
store_grp_all_agg = final_merge.groupBy("org","VBS","DIV","LINE","item_class","season_cd","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm")
			) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
	.withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
store_grp_all_agg_class = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                              .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                              .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("plan_name",lit("ALL"))\
                                              .withColumn("store_grp_1",lit("ALL"))											  
											  
store_grp_all_agg_class = store_grp_all_agg_class.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")

################################################ Aggregation at line level ###############################										  
store_grp_all_agg = final_merge.groupBy("org","VBS","DIV","LINE","season_cd","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm"),
			) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
	.withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
store_grp_all_agg_line = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
											 .withColumn("store_grp_1",lit("ALL"))
											 
store_grp_all_agg_line = store_grp_all_agg_line.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")
											 
#################################### Aggregation at division level #############################################											 
store_grp_all_agg = final_merge.groupBy("org","VBS","DIV","season_cd","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm"),
			) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
	.withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
store_grp_all_agg_div = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL"))\
											 .withColumn("store_grp_1",lit("ALL"))
											 
store_grp_all_agg_div = store_grp_all_agg_div.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")
											 
########################################### Aggregation at VBS level ########################################											 
store_grp_all_agg = final_merge.groupBy("org","VBS","season_cd","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm"),
			) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
    .withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
	
store_grp_all_agg_vbs = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL"))\
                                             .withColumn("DIV",lit("ALL"))\
											 .withColumn("store_grp_1",lit("ALL"))
											 
store_grp_all_agg_vbs = store_grp_all_agg_vbs.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")
											 
											 
################################################## Aggregation at Org level #########################################									 
store_grp_all_agg = final_merge.groupBy("org","season_cd","scenario","md_Depth").\
         agg(F.sum(final_merge.reg_price * final_merge.initial_buy).alias("prod_reg_price"),
            F.sum(final_merge.net_rev).alias("net_rev"),
            F.sum(final_merge.tot_cost).alias("tot_cost"),
            F.sum(final_merge.final_oh).alias("final_oh"),
            F.sum(final_merge.initial_buy).alias("initial_buy"),
            F.avg(final_merge.current_disc).alias("current_disc"),
            F.sum(((1-(final_merge.MD1/100.0))*final_merge.reg_price) * final_merge.md1_units).alias("md1_crp_prod"),
            F.sum(((1-(final_merge.MD2/100.0))*final_merge.reg_price) * final_merge.md2_units).alias("md2_crp_prod"),
            F.sum(((1-(final_merge.MD3/100.0))*final_merge.reg_price) * final_merge.md3_units).alias("md3_crp_prod"),
            F.sum(((1-(final_merge.MD4/100.0))*final_merge.reg_price) * final_merge.md4_units).alias("md4_crp_prod"),
            F.sum(((1-(final_merge.MD5/100.0))*final_merge.reg_price) * final_merge.md5_units).alias("md5_crp_prod"),
            F.sum(final_merge.md1_units).alias("md1_units"),
            F.sum(final_merge.md2_units).alias("md2_units"),
            F.sum(final_merge.md3_units).alias("md3_units"),
            F.sum(final_merge.md4_units).alias("md4_units"),
            F.sum(final_merge.md5_units).alias("md5_units"),
            F.sum(final_merge.salvage).alias("salvage"),
            F.sum(final_merge.md1_rev).alias("md1_rev"),
            F.sum(final_merge.md2_rev).alias("md2_rev"),
            F.sum(final_merge.md3_rev).alias("md3_rev"),
            F.sum(final_merge.md4_rev).alias("md4_rev"),
            F.sum(final_merge.md5_rev).alias("md5_rev"),
            F.sum(final_merge.md1_cmd).alias("md1_cmd"),
            F.sum(final_merge.md2_cmd).alias("md2_cmd"),
            F.sum(final_merge.md3_cmd).alias("md3_cmd"), 
            F.sum(final_merge.md4_cmd).alias("md4_cmd"),
            F.sum(final_merge.md5_cmd).alias("md5_cmd"),
            F.sum(final_merge.md1_oh).alias("md1_oh"),
            F.sum(final_merge.md2_oh).alias("md2_oh"),
            F.sum(final_merge.md3_oh).alias("md3_oh"), 
            F.sum(final_merge.md4_oh).alias("md4_oh"),
            F.sum(final_merge.md5_oh).alias("md5_oh"),
            F.sum(final_merge.md1_cost).alias("md1_cost"),
            F.sum(final_merge.md2_cost).alias("md2_cost"),
            F.sum(final_merge.md3_cost).alias("md3_cost"), 
            F.sum(final_merge.md4_cost).alias("md4_cost"),
            F.sum(final_merge.md5_cost).alias("md5_cost"),
            F.sum(final_merge.total_units).alias("total_units"),
            F.sum(final_merge.total_cmd).alias("total_cmd"),
            F.sum(final_merge.md1_gma).alias("md1_gma"),
            F.sum(final_merge.md2_gma).alias("md2_gma"),
            F.sum(final_merge.md3_gma).alias("md3_gma"),
            F.sum(final_merge.md4_gma).alias("md4_gma"),
            F.sum(final_merge.md5_gma).alias("md5_gma"),
            F.sum(final_merge.gma_final).alias("total_gm"),
			) 
    
    
    
    
store_grp_all_agg = store_grp_all_agg.withColumn("final_gm",((store_grp_all_agg.net_rev-store_grp_all_agg.tot_cost)/ \
                      store_grp_all_agg.net_rev))\
    .withColumn("final_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.final_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md1_gm",((store_grp_all_agg.md1_rev-store_grp_all_agg.md1_cost)/store_grp_all_agg.md1_rev))\
    .withColumn("md2_gm",F.when(store_grp_all_agg.md2_rev>0,(store_grp_all_agg.md2_rev-store_grp_all_agg.md2_cost)/store_grp_all_agg.md2_rev).otherwise(0))\
    .withColumn("md3_gm",F.when(store_grp_all_agg.md3_rev>0,(store_grp_all_agg.md3_rev-store_grp_all_agg.md3_cost)/store_grp_all_agg.md3_rev).otherwise(0))\
    .withColumn("md4_gm",F.when(store_grp_all_agg.md4_rev>0,(store_grp_all_agg.md4_rev-store_grp_all_agg.md4_cost)/store_grp_all_agg.md4_rev).otherwise(0))\
    .withColumn("md5_gm",F.when(store_grp_all_agg.md5_rev>0,(store_grp_all_agg.md5_rev-store_grp_all_agg.md5_cost)/store_grp_all_agg.md5_rev).otherwise(0))\
    .withColumn("md1_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md1_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md2_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md2_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md3_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md3_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md4_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md4_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("md5_st",(store_grp_all_agg.initial_buy-store_grp_all_agg.md5_oh)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_reg",(store_grp_all_agg.prod_reg_price)/store_grp_all_agg.initial_buy)\
    .withColumn("weighted_md1_price",(store_grp_all_agg.md1_crp_prod)/store_grp_all_agg.md1_units)\
    .withColumn("weighted_md2_price",(store_grp_all_agg.md2_crp_prod)/store_grp_all_agg.md2_units)\
    .withColumn("weighted_md3_price",(store_grp_all_agg.md3_crp_prod)/store_grp_all_agg.md3_units)\
    .withColumn("weighted_md4_price",(store_grp_all_agg.md4_crp_prod)/store_grp_all_agg.md4_units)\
    .withColumn("weighted_md5_price",(store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.md5_units)\
	.withColumn("overall_md_crp",((store_grp_all_agg.md1_crp_prod+store_grp_all_agg.md2_crp_prod+store_grp_all_agg.md3_crp_prod+store_grp_all_agg.md4_crp_prod+store_grp_all_agg.md5_crp_prod)/store_grp_all_agg.total_units))
    
store_grp_all_agg_all = store_grp_all_agg.withColumn("md1", (1-(store_grp_all_agg.weighted_md1_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md2", (1-(store_grp_all_agg.weighted_md2_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md3", (1-(store_grp_all_agg.weighted_md3_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md4", (1-(store_grp_all_agg.weighted_md4_price/store_grp_all_agg.weighted_reg))*100)\
                                             .withColumn("md5", (1-(store_grp_all_agg.weighted_md5_price/store_grp_all_agg.weighted_reg))*100)\
											  .withColumn("md_overall", (1-(store_grp_all_agg.overall_md_crp/store_grp_all_agg.weighted_reg))*100).withColumn("plan_name",lit("ALL"))\
											 .withColumn("item_class",lit("ALL"))\
                                             .withColumn("LINE",lit("ALL")).withColumn("DIV",lit("ALL")).withColumn("VBS",lit("ALL")).withColumn("store_grp_1",lit("ALL"))

store_grp_all_agg_all = store_grp_all_agg_all.select("org" ,"VBS","DIV","LINE","item_class","plan_name","season_cd","store_grp_1","scenario","md_Depth","net_rev","tot_cost","final_oh","initial_buy","current_disc","md1_units","md2_units","md3_units","md4_units","md5_units","salvage","md1_rev","md2_rev","md3_rev","md4_rev","md5_rev","md1_cmd","md2_cmd","md3_cmd","md4_cmd","md5_cmd","md1_oh","md2_oh","md3_oh","md4_oh","md5_oh","md1_cost","md2_cost","md3_cost","md4_cost","md5_cost","total_units","total_cmd","md1_gma","md2_gma","md3_gma","md4_gma","md5_gma","total_gm","final_gm","final_st","md1_gm","md2_gm","md3_gm","md4_gm","md5_gm","md1_st","md2_st","md3_st","md4_st","md5_st","weighted_reg","weighted_md1_price","weighted_md2_price","weighted_md3_price","weighted_md4_price","weighted_md5_price","md1","md2","md3","md4","md5","md_overall","overall_md_crp")


mas1 = store_grp_all_agg_pln.unionAll(store_grp_all_agg_class)
mas2 = mas1.unionAll(store_grp_all_agg_line)
mas3 = mas2.unionAll(store_grp_all_agg_div)
mas4 = mas3.unionAll(store_grp_all_agg_vbs)
mas5 = mas4.unionAll(store_grp_all_agg_all)
mass1 = all_store_agg_pln.unionAll(all_store_agg_class)
mass2 = mass1.unionAll(all_store_agg_line)
mass3 = mass2.unionAll(all_store_agg_div)
mass4 = mass3.unionAll(all_store_agg_vbs)
mass5 = mass4.unionAll(all_store_agg_all)

final_output_data = mas5.unionAll(mass5)


md1_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit(1).alias("md_seq"),final_output_data.md1.alias("md_perc"),final_output_data.weighted_md1_price.alias("unit_price"),final_output_data.md1_units.alias("units_sold"),final_output_data.md1_rev.alias("dlr_rev"),final_output_data.md1_st.alias("perc_sellthrough"),final_output_data.md1_oh.alias("unit_onhand"),final_output_data.md1_gma.alias("dlr_margin"),final_output_data.md1_gm.alias("perc_margin"),final_output_data.md1_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))

overall_md_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit("overall").alias("md_seq"),final_output_data.md_overall.alias("md_perc"),final_output_data.overall_md_crp.alias("unit_price"),final_output_data.total_units.alias("units_sold"),(final_output_data.net_rev).alias("dlr_rev"),final_output_data.final_st.alias("perc_sellthrough"),final_output_data.final_oh.alias("unit_onhand"),final_output_data.total_gm.alias("dlr_margin"),final_output_data.final_gm.alias("perc_margin"),final_output_data.total_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))
                                         
										 
md2_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit(2).alias("md_seq"),final_output_data.md2.alias("md_perc"),final_output_data.weighted_md2_price.alias("unit_price"),final_output_data.md2_units.alias("units_sold"),final_output_data.md2_rev.alias("dlr_rev"),final_output_data.md2_st.alias("perc_sellthrough"),final_output_data.md2_oh.alias("unit_onhand"),final_output_data.md2_gma.alias("dlr_margin"),final_output_data.md2_gm.alias("perc_margin"),final_output_data.md2_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))


md3_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit(3).alias("md_seq"),final_output_data.md3.alias("md_perc"),final_output_data.weighted_md3_price.alias("unit_price"),final_output_data.md3_units.alias("units_sold"),final_output_data.md3_rev.alias("dlr_rev"),final_output_data.md3_st.alias("perc_sellthrough"),final_output_data.md3_oh.alias("unit_onhand"),final_output_data.md3_gma.alias("dlr_margin"),final_output_data.md3_gm.alias("perc_margin"),final_output_data.md3_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))

md4_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit(4).alias("md_seq"),final_output_data.md4.alias("md_perc"),final_output_data.weighted_md4_price.alias("unit_price"),final_output_data.md4_units.alias("units_sold"),final_output_data.md4_rev.alias("dlr_rev"),final_output_data.md4_st.alias("perc_sellthrough"),final_output_data.md4_oh.alias("unit_onhand"),final_output_data.md4_gma.alias("dlr_margin"),final_output_data.md4_gm.alias("perc_margin"),final_output_data.md4_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))

md5_metrics = final_output_data.select("org" ,"VBS","DIV","LINE","item_class","season_cd","store_grp_1","plan_name" ,"scenario","md_Depth",lit(5).alias("md_seq"),final_output_data.md5.alias("md_perc"),final_output_data.weighted_md5_price.alias("unit_price"),final_output_data.md5_units.alias("units_sold"),final_output_data.md5_rev.alias("dlr_rev"),final_output_data.md5_st.alias("perc_sellthrough"),final_output_data.md5_oh.alias("unit_onhand"),final_output_data.md5_gma.alias("dlr_margin"),final_output_data.md5_gm.alias("perc_margin"),final_output_data.md5_cmd.alias("dlr_cmd"),final_output_data.final_gm.alias("pane_marg_rate"),final_output_data.final_st.alias("pane_fin_st"),final_output_data.total_cmd.alias("pane_cmd"),final_output_data.net_rev.alias("pane_revenue"))

#union all aggregated data frames
trans1 = md1_metrics.unionAll(md2_metrics)
trans2 = trans1.unionAll(md3_metrics)
trans3 = trans2.unionAll(md4_metrics)
trans4 = trans3.unionAll(overall_md_metrics)
final_transposed_data = trans4.unionAll(md5_metrics)

dmap = sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = '\t').load('gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv')
#dmap.saveAsTable('dmap')
dmap1_vbs = dmap.select("h1","h1_desc").distinct()
dmap1_div = dmap.select("h2","h2_desc").distinct()
dmap1_line = dmap.select("h3",dmap.h2.alias("hh2"),dmap.h1.alias("hh1"),"h3_desc").distinct()
dmap1_c = dmap.select("h4",dmap.h3.alias("hhh3"), dmap.h2.alias("hhh2"),dmap.h1.alias("hhh1"),"h4_desc").distinct().orderBy("h4_desc")
dmap1_cl = dmap1_c.groupBy("hhh1","hhh2","hhh3","h4").agg(F.collect_list(dmap1_c.h4_desc).alias("h4_ds"))
def concatinator(l):
    l = list(sorted(l))
    return('/'.join(l))
udf_concat = udf(concatinator,StringType())

#Joining Hierarchy description
dmap1_cls = dmap1_cl.select("hhh1","hhh2","hhh3","h4",udf_concat(dmap1_cl.h4_ds).alias("h4_desc"))
final_transposed_data_a =  final_transposed_data.join(dmap1_vbs,final_transposed_data.VBS == dmap1_vbs.h1,'left')
final_transposed_data_b =  final_transposed_data_a.join(dmap1_div,final_transposed_data_a.DIV == dmap1_div.h2,'left')
final_transposed_data_c =  final_transposed_data_b.join(dmap1_line,[(final_transposed_data_b.LINE == dmap1_line.h3) & (final_transposed_data_b.DIV == dmap1_line.hh2) & (final_transposed_data_b.VBS == dmap1_line.hh1)],'left')
final_transposed_data_x =  final_transposed_data_c.join(dmap1_cls,[(final_transposed_data_c.item_class == dmap1_cls.h4) & (final_transposed_data_c.LINE == dmap1_cls.hhh3) & (final_transposed_data_c.VBS == dmap1_cls.hhh1) & (final_transposed_data_c.DIV == dmap1_cls.hhh2)],'left')

final_transposed_data_1 = final_transposed_data_x.withColumn("scenario_type",F.when(final_transposed_data_x.scenario < 1,"optimal").otherwise("custom"))
comp1_data = final_transposed_data_1.filter("org like 'S%'")
comp2_data = final_transposed_data_1.filter("org like 'K%'")
s = comp1_data.count()
k = comp2_data.count()
comp1_data_f = comp1_data.select("org",F.when(comp1_data.VBS == 'ALL','ALL').otherwise(concat(comp1_data.VBS,lit(" - "),comp1_data.h1_desc)).alias("business"),F.when(comp1_data.DIV == 'ALL','ALL').otherwise(concat(comp1_data.DIV,lit(" - "),comp1_data.h2_desc)).alias("division"),F.when(comp1_data.LINE == 'ALL','ALL').otherwise(concat(comp1_data.LINE,lit(" - "),comp1_data.h3_desc)).alias("line_cat"),F.when(comp1_data.item_class == 'ALL','ALL').otherwise(concat(comp1_data.item_class,lit(" - "),comp1_data.h4_desc)).alias("cls_subcat"),comp1_data.season_cd.alias("season"),upper(comp1_data.store_grp_1).alias("store_grp"),comp1_data.plan_name.alias("plan_id"),"scenario_type",F.when(comp1_data.md_Depth == 0,"Optimal").otherwise(concat(lit("Scenario "),col("scenario"),lit("["),col("md_Depth"),lit("%]"))).alias("scenario_id"), "md_seq",udf_int_rd(comp1_data.md_perc).alias("md_perc"),udf_rd(comp1_data.unit_price,lit(2)).alias("unit_price"),udf_int_rd(comp1_data.units_sold).alias("units_sold"),udf_int_rd(comp1_data.dlr_rev).alias("dlr_rev"),udf_int_rd(comp1_data.perc_sellthrough * 100).alias("perc_sellthrough"),udf_int_rd(comp1_data.unit_onhand).alias("unit_onhand"),udf_int_rd(comp1_data.dlr_margin).alias("dlr_margin"),udf_int_rd(comp1_data.perc_margin * 100).alias("perc_margin"),udf_int_rd(comp1_data.dlr_cmd).alias("dlr_cmd"),udf_int_rd(comp1_data.pane_marg_rate * 100).alias("pane_marg_rate"),udf_int_rd(comp1_data.pane_fin_st * 100).alias("pane_fin_st"),udf_int_rd(comp1_data.pane_cmd).alias("pane_cmd"),udf_int_rd(comp1_data.pane_revenue).alias("pane_revenue")).orderBy(comp1_data.md_Depth)
comp2_data_f = comp2_data.select("org",F.when(comp2_data.VBS == 'ALL','ALL').otherwise(concat(comp2_data.VBS,lit(" - "),comp2_data.h1_desc)).alias("business"),comp2_data.DIV.alias("division"),F.when(comp2_data.LINE == 'ALL','ALL').otherwise(concat(comp2_data.LINE,lit(" - "),comp2_data.h3_desc)).alias("line_cat"),F.when(comp2_data.item_class == 'ALL','ALL').otherwise(concat(comp2_data.item_class,lit(" - "),comp2_data.h4_desc)).alias("cls_subcat"),comp2_data.season_cd.alias("season"),upper(comp2_data.store_grp_1).alias("store_grp"),comp2_data.plan_name.alias("plan_id"),"scenario_type",F.when(comp2_data.md_Depth == 0,"Optimal").otherwise(concat(lit("Scenario "),col("scenario"),lit("["),col("md_Depth"),lit("%]"))).alias("scenario_id"), "md_seq",udf_int_rd(comp2_data.md_perc).alias("md_perc"),udf_rd(comp2_data.unit_price,lit(2)).alias("unit_price"),udf_int_rd(comp2_data.units_sold).alias("units_sold"),udf_int_rd(comp2_data.dlr_rev).alias("dlr_rev"),udf_int_rd(comp2_data.perc_sellthrough * 100).alias("perc_sellthrough"),udf_int_rd(comp2_data.unit_onhand).alias("unit_onhand"),udf_int_rd(comp2_data.dlr_margin).alias("dlr_margin"),udf_int_rd(comp2_data.perc_margin * 100).alias("perc_margin"),udf_int_rd(comp2_data.dlr_cmd).alias("dlr_cmd"),udf_int_rd(comp2_data.pane_marg_rate * 100).alias("pane_marg_rate"),udf_int_rd(comp2_data.pane_fin_st * 100).alias("pane_fin_st"),udf_int_rd(comp2_data.pane_cmd).alias("pane_cmd"),udf_int_rd(comp2_data.pane_revenue).alias("pane_revenue")).orderBy(comp2_data.md_Depth)



final_transposed_data_2 = comp1_data_f.unionAll(comp2_data_f)

dummy_md= sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = ',').load('gs://affine_poc/skprccm/dummy_data/comp1_md_level.csv')

comp2_dummy_md = dummy_md.filter("a like 'K%'")
comp1_dummy_md = dummy_md.filter("a like 'S%'")
if s == 0:
    final_transposed_data_f = final_transposed_data_2.unionAll(comp1_dummy_md)
elif k == 0:
    final_transposed_data_f = final_transposed_data_2.unionAll(comp2_dummy_md)
else:
    final_transposed_data_f = final_transposed_data_2

#writing Data into hive table
final_transposed_data_f.write.saveAsTable('final_transposed_data')

