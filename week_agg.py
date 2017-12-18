#Importing necessary Libraries
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


fcl_yr = 2016

final_merge_week = HiveContext.sql("select * from week_level_extracted_table")


def splitstring(a):
    lm = [int(i) for i in a.split("-")]
    return(lm)
udf1 = udf(splitstring,ArrayType(IntegerType()))

final_merge_week = final_merge_week.select("key","org",udf1(final_merge_week.key)[0].alias("vbs"),udf1(final_merge_week.key)[1].alias("div"),udf1(final_merge_week.key)[2].alias("line"),udf1(final_merge_week.key)[3].alias("item_class"),"plan_name","season_cd","store_grp","md_depth","scenario","week","wk_revenue","margin_dlr","cmd_dlr","units_sold","current_oh","ib")





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


#Aggregation at Plan and Store group level 
all_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","item_class","plan_name","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_plan = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)
 
# all_store_agg_wk_plan.show(5)
 
 
 #class  and Store group level
all_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","item_class","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_class = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)
 
 
#Line  and Store group level
all_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_line = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)
 

#DIV  and Store group level
all_store_agg_wk=final_merge_week.groupBy("org","vbs","div","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_div = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)


#VBS  and Store group level
all_store_agg_wk=final_merge_week.groupBy("org","vbs","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_vbs = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("DIV",lit("ALL")).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)


#Org  and Store group level
all_store_agg_wk=final_merge_week.groupBy("org","season_cd",F.lower(final_merge_week.store_grp).alias("store_grp"),"md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_store_agg_wk_org = all_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_store_agg_wk.unit_onhand/all_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_store_agg_wk.units_sold/(all_store_agg_wk.units_sold+all_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_store_agg_wk.dlr_mar/all_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div",lit("ALL")).withColumn("vbs",lit("ALL")).withColumn("unit_wos",all_store_agg_wk.unit_onhand/all_store_agg_wk.units_sold)



#Everything without store_grp

#Plan level 
all_no_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","item_class","plan_name","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)


all_no_store_agg_wk_plan = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("store_grp",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)
 

 
 
 #class level
all_no_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","item_class","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_no_store_agg_wk_class = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)
 
 
#Line level
all_no_store_agg_wk=final_merge_week.groupBy("org","vbs","div","line","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_no_store_agg_wk_line = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)
 

#Division level
all_no_store_agg_wk=final_merge_week.groupBy("org","vbs","div","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_no_store_agg_wk_div = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)


#VBS level
all_no_store_agg_wk=final_merge_week.groupBy("org","vbs","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_no_store_agg_wk_vbs = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("DIV",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)


#Org level
all_no_store_agg_wk=final_merge_week.groupBy("org","season_cd","md_depth","scenario","week").agg(
                    F.sum(final_merge_week.wk_revenue).alias("dlr_rev"),
					F.sum(final_merge_week.margin_dlr).alias("dlr_mar"),
					F.sum(final_merge_week.cmd_dlr).alias("dlr_cmd"),
					F.sum(final_merge_week.units_sold).alias("units_sold"),
					F.sum(final_merge_week.current_oh).alias("unit_onhand"),
					F.sum(final_merge_week.ib).alias("ib"),
)
all_no_store_agg_wk_org = all_no_store_agg_wk.withColumn("perc_sellthrough" ,1 - (all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.ib)).withColumn("perc_checkout_rate" ,(all_no_store_agg_wk.units_sold/(all_no_store_agg_wk.units_sold+all_no_store_agg_wk.unit_onhand))).withColumn("perc_marg_rate",all_no_store_agg_wk.dlr_mar/all_no_store_agg_wk.dlr_rev).withColumn("plan_name",lit("ALL")).withColumn("store_grp",lit("ALL")).withColumn("item_class",lit("ALL")).withColumn("line",lit("ALL")).withColumn("div",lit("ALL")).withColumn("vbs",lit("ALL")).withColumn("unit_wos",all_no_store_agg_wk.unit_onhand/all_no_store_agg_wk.units_sold)

all_store_agg_wk_plan = all_store_agg_wk_plan.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_store_agg_wk_class = all_store_agg_wk_class.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_store_agg_wk_line = all_store_agg_wk_line.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_store_agg_wk_div = all_store_agg_wk_div.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_store_agg_wk_vbs = all_store_agg_wk_vbs.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_store_agg_wk_org = all_store_agg_wk_org.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")


all_no_store_agg_wk_plan = all_no_store_agg_wk_plan.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_no_store_agg_wk_class = all_no_store_agg_wk_class.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_no_store_agg_wk_line = all_no_store_agg_wk_line.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_no_store_agg_wk_div = all_no_store_agg_wk_div.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_no_store_agg_wk_vbs = all_no_store_agg_wk_vbs.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")

all_no_store_agg_wk_org = all_no_store_agg_wk_org.select("org","vbs","div","line","item_class","season_cd","store_grp","plan_name","scenario","md_depth","week","dlr_rev","dlr_mar","dlr_cmd","units_sold","unit_onhand","perc_sellthrough","perc_marg_rate","perc_checkout_rate","unit_wos")







#union all aggregated tables
mas1 = all_store_agg_wk_plan.unionAll(all_store_agg_wk_class)
mas2 = mas1.unionAll(all_store_agg_wk_line)
mas3 = mas2.unionAll(all_store_agg_wk_div)
mas4 = mas3.unionAll(all_store_agg_wk_vbs)
mas5 = mas4.unionAll(all_store_agg_wk_org)
mass1 = all_no_store_agg_wk_plan.unionAll(all_no_store_agg_wk_class)
mass2 = mass1.unionAll(all_no_store_agg_wk_line)
mass3 = mass2.unionAll(all_no_store_agg_wk_div)
mass4 = mass3.unionAll(all_no_store_agg_wk_vbs)
mass5 = mass4.unionAll(all_no_store_agg_wk_org)

final_week_agg_data = mas5.unionAll(mass5)

dmap = sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = '\t').load('gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv')
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
def scenario_sorter(x):
    if(x == "Optimal"):
	    return 0
    if(x == "Scenario 1[-15%]"):
	    return 1
    if(x == "Scenario 1[-10%]"):
	    return 2
    if(x == "Scenario 1[-5%]"):
	    return 3
    if(x == "Scenario 1[5%]"):
	    return 4
    if(x == "Scenario 1[10%]"):
	    return 6
    if(x == "Scenario 1[15%]"):
	    return 7
    
    if(x == "Scenario 2[-15%]"):
	    return 8
    if(x == "Scenario 2[-10%]"):
	    return 9
    if(x == "Scenario 2[-5%]"):
	    return 10
    if(x == "Scenario 2[5%]"):
	    return 11
    if(x == "Scenario 2[10%]"):
	    return 12
    if(x == "Scenario 2[15%]"):
	    return 13

    if(x == "Scenario 3[-15%]"):
	    return 14
    if(x == "Scenario 3[-10%]"):
	    return 15
    if(x == "Scenario 3[-5%]"):
	    return 16
    if(x == "Scenario 3[5%]"):
	    return 17
    if(x == "Scenario 3[10%]"):
	    return 18
    if(x == "Scenario 3[15%]"):
	    return 19


udf_sorter = udf(scenario_sorter,IntegerType())


# Adding hierarchy description 
final_week_agg_data_a =  final_week_agg_data.join(dmap1_vbs,final_week_agg_data.vbs == dmap1_vbs.h1,'left')
final_week_agg_data_b =  final_week_agg_data_a.join(dmap1_div,final_week_agg_data_a.div == dmap1_div.h2,'left')

final_week_agg_data_c =  final_week_agg_data_b.join(dmap1_line,[(final_week_agg_data_b.line == dmap1_line.h3) & (final_week_agg_data_b.vbs == dmap1_line.hh1) & (final_week_agg_data_b.div == dmap1_line.hh2)],'left')
final_week_agg_data_x  =  final_week_agg_data_c.join(dmap1_cls,[(final_week_agg_data_c.item_class == dmap1_cls.h4) & (final_week_agg_data_c.line == dmap1_cls.hhh3) & (final_week_agg_data_c.vbs == dmap1_cls.hhh1) & (final_week_agg_data_c.div == dmap1_cls.hhh2)],'left')



final_week_agg_data_1 = final_week_agg_data_x.withColumn("scenario_type",F.when(final_week_agg_data_x.scenario < 1,"optimal").otherwise("custom"))
comp1_data = final_week_agg_data_1.filter("org like 'S%'")
comp2_data = final_week_agg_data_1.filter("org like 'K%'")
s= comp1_data.count()
k= comp2_data.count()
comp1_data_f = comp1_data.select("org", F.when(comp1_data.vbs == 'ALL','ALL').otherwise(concat(comp1_data.vbs,lit(" - "),coalesce(comp1_data.h1_desc,lit("")))).alias("business"),F.when(comp1_data.div == 'ALL','ALL').otherwise(concat(comp1_data.div,lit(" - "),comp1_data.h2_desc)).alias("division"),F.when(comp1_data.line == 'ALL','ALL').otherwise(concat(comp1_data.line,lit(" - "),comp1_data.h3_desc)).alias("line_cat"), F.when(comp1_data.item_class == 'ALL','ALL').otherwise(concat(comp1_data.item_class,lit(" - "),comp1_data.h4_desc)).alias("cls_subcat"), comp1_data.season_cd.alias("season"),upper(comp1_data.store_grp).alias("store_grp"), comp1_data.plan_name.alias("plan_id"),"scenario_type",F.when(comp1_data.md_depth == 0,"Optimal").otherwise(concat(lit("Scenario "),col("scenario"),lit("["),col("md_depth"),lit("%]"))).alias("scenario_id"),F.when(comp1_data.week > 52,udf_int_rd(((comp1_data.week)-52) + (fcl_yr+1)*100)).otherwise(udf_int_rd(((comp1_data.week)) + (fcl_yr)*100)).alias("week") ,udf_int_rd(comp1_data.dlr_rev).alias("dlr_rev"),udf_int_rd(comp1_data.dlr_mar).alias("dlr_mar"),udf_int_rd(comp1_data.dlr_cmd).alias("dlr_cmd"),udf_int_rd(comp1_data.units_sold).alias("units_sold"),udf_int_rd(comp1_data.unit_onhand).alias("unit_onhand"),udf_int_rd(comp1_data.perc_sellthrough * 100).alias("perc_sellthrough"),udf_int_rd(comp1_data.perc_marg_rate * 100).alias("perc_marg_rate"),udf_int_rd(comp1_data.perc_checkout_rate * 100).alias("perc_checkout_rate"))
comp2_data_f = comp2_data.select("org", F.when(comp2_data.vbs == 'ALL','ALL').otherwise(concat(comp2_data.vbs,lit(" - "),coalesce(comp2_data.h1_desc,lit("")))).alias("business"),comp2_data.div.alias("division"),F.when(comp2_data.line == 'ALL','ALL').otherwise(concat(comp2_data.line,lit(" - "),comp2_data.h3_desc)).alias("line_cat"), F.when(comp2_data.item_class == 'ALL','ALL').otherwise(concat(comp2_data.item_class,lit(" - "),comp2_data.h4_desc)).alias("cls_subcat"), comp2_data.season_cd.alias("season"),upper(comp2_data.store_grp).alias("store_grp"), comp2_data.plan_name.alias("plan_id"),"scenario_type",F.when(comp2_data.md_depth == 0,"Optimal").otherwise(concat(lit("Scenario "),col("scenario"),lit("["),col("md_depth"),lit("%]"))).alias("scenario_id"),F.when(comp2_data.week > 52,udf_int_rd(((comp2_data.week)-52) + (fcl_yr+1)*100)).otherwise(udf_int_rd(((comp2_data.week)) + (fcl_yr)*100)).alias("week") ,udf_int_rd(comp2_data.dlr_rev).alias("dlr_rev"),udf_int_rd(comp2_data.dlr_mar).alias("dlr_mar"),udf_int_rd(comp2_data.dlr_cmd).alias("dlr_cmd"),udf_int_rd(comp2_data.units_sold).alias("units_sold"),udf_int_rd(comp2_data.unit_onhand).alias("unit_onhand"),udf_int_rd(comp2_data.perc_sellthrough * 100).alias("perc_sellthrough"),udf_int_rd(comp2_data.perc_marg_rate * 100).alias("perc_marg_rate"),udf_int_rd(comp2_data.perc_checkout_rate * 100).alias("perc_checkout_rate"))
final_week_agg_data_2 = comp1_data_f.unionAll(comp2_data_f)
final_week_agg_data_2  = final_week_agg_data_2.orderBy(udf_sorter(final_week_agg_data_2.scenario_id))

dummy_wk= sqlContext.read.format("com.databricks.spark.csv").options(header = 'true',inferschema = 'true',delimiter = ',').load('gs://affine_poc/skprccm/dummy_data/week_dummy_data.csv')

comp2_dummy_wk = dummy_wk.filter("a like 'K%'")
comp1_dummy_wk = dummy_wk.filter("a like 'S%'")
if s == 0:
    final_week_agg_data_3 = final_week_agg_data_2.unionAll(comp1_dummy_wk)
elif k == 0:
    final_week_agg_data_3 = final_week_agg_data_2.unionAll(comp2_dummy_wk)
else:
    final_week_agg_data_3 = final_week_agg_data_2
final_week_agg_data_3.write.saveAsTable('week_agg_final_x')

