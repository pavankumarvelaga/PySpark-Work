#Importing Libraries
import os
import sys
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
basics_list = ["C","T","1","6","9"]

temp = len(sys.argv)
file_name = "select * from " + sys.argv[temp-1]

#Selecting the tables
d1 = HiveContext.sql(file_name)

d1 = d1.filter("md_count <=5").select("h1","h2","h3","h5","store","reg_price","disc","on_hand","predicted_sale","md_week","md_count","key","sign_factor","initial_pe",coalesce(d1.initial_buy.cast("float"),lit(0)).alias("initial_buy"),"sub","sal_value","high_disc_fact","h_dow_fac","oh_breakage","ptd_ratio","clearance_flag_final","egp_flag","seasonality","circular",coalesce(d1.cost.cast("float"),lit(0)).alias("cost"),"season_cd","h4","plan_name","store_grp",upper(d1.org).alias("org"),"fwk_extra_percent_off","fwk_extra_pc_off_adj","fwk_extra_pc_off","wow_decline").repartition("key")

#Function to split a string
def splitstring(a):
    lm = [int(i) for i in a.split("-")]
    return(lm)



#Function to round a given number
def rd(a,b):
    x = a * math.pow(10,b+1)
    if x%(math.pow(10,b)) > 5:
        l = math.ceil(x/10)
    else:
        l = math.floor(x/10)
    return(float(l)/math.pow(10,b))

#Function to find the maximum of two numbers	
def max_value(a, b):
    if a > b:
        return a
    else:
        return b
def min_value(a, b):
    if a > b:
        return b
    else:
        return a

AD_rdd=d1.rdd

#Function to extract key value pairs
def splitterv2(x,y,a):
    inter = (str.split(a,x))
    inter2 = [str.split(w,y) for w in inter][1:]
    key = list(map(lambda x : x[0],inter2))
    values = list(map(lambda x : x[1],inter2))
    if (len(a)>150):
        key = list(map(float,key))
        values=list(map(float,values))
    else:
        key = list(map(int,key))
        values=list(map(float,values))
    ans = dict(zip(key,values))
    return (ans)

def splitterv3(x,y,a):
    inter = (str.split(a,x))
    inter2 = [str.split(w,y) for w in inter][1:]
    key = list(map(lambda x : x[0],inter2))
    values = list(map(lambda x : x[1],inter2))
    key = list(map(int,key))
    values=list(map(float,values))
    ans = dict(zip(key,values))
    return (ans)


#Function to calculate the revenue for given discount combination	
def process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag
                ,seasonalitycoeff,reg_price,md1,md2,md3,md4,md5,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit):    
    md_count = md_count
    tempRevenue = 0.00
    tempst = 0.00

    arr_FinalCondmd1 = 0.00
    arr_FinalCondmd2 = 0.00
    arr_FinalCondmd3 = 0.00
    arr_FinalCondmd4 = 0.00
    arr_FinalCondmd5 = 0.00
    arraymax = []
    arr_FinalCondrev = 0.00
    arr_FinalCondoh = 0.00
    arr_Finalps = 0.00
    arr_FinalCondib = 0.00
    arr_FinalCondcd = 0.00
    arr_FinalCondsalvage = 0.00
    week_cnt = 0
    revenue = 0.00
    tot_revenue = 0.00
    ticketingsum = 0.00
    tot_margin = 0.0
    netrevenue=0.00
    salvagefinal=0.00
    df = 0.00
    units = 0.00
    maxrev= 0.00
    breakage= 1.00
    decline = 0.45
    wow_decline = 1.00
    wk_rev_lst = []
    wk_oh_lst = []
    strMaster = 0.00
    
    price_overall = 0.00
    dfk = 0
    checkout_rate = 0.0
    arr_FinalCondst = ""
    di = discount
    wow = Predicted_Units
    week_table = []
    oh_curr = OnHnd - Predicted_Units
    oh_int = OnHnd
    oh_prev = OnHnd
    week = mdt1
    current_week = str(week)
    last_week = str(week)
    md_no = 0
#Week loop starts here
    for week in range(mdt1,mos+1):
        ticketing = ticketing
        cmd = 0.00
        
        seasonalitycoeff1 = seasonalitycoeff

        if week < mdt2:
            df=float(md1)/100
            dfk =md1
            md_no = 1
        elif week >= mdt2 and week < mdt3:
            df=float(md2)/100
            dfk =md2
            md_no = 2
        elif week >= mdt3 and week < mdt4:
            df=float(md3)/100
            dfk = md3
            md_no = 3
        elif week >= mdt4 and week < mdt5:
            df = float(md4)/100
            dfk = md4
            md_no = 4
        else:
            df = float(md5)/100
            dfk = md5
            md_no = 5

        
        if (week == mdt1 or week == mdt2 or week == mdt3 or week == mdt4 or week == mdt5):
            ticketing = ticketing
            elas_factor = math.pow((1 - df) / (1 - di), (elasticity * final_hd_map.get(dfk,1)))
            csign = 1.10
            
        else:
            ticketing = 0.00
            elas_factor = 1.00
            csign = 1.00
            
            
        if (week == mdt1 or week == mdt2 or week == mdt3 or week == mdt4 or week == mdt5):
            wow_decline = math.pow((((decline * elas_factor) + (1-decline)) / elas_factor), 0.3333)
            eff_wow_decline = 1.00

        elif((week == mdt1 + 1) or (week == (mdt1 + 2) or week == (mdt1 + 3) or week == (mdt2 + 1) or week == (mdt2 + 2) or week == (mdt2 + 3) or week == (mdt3 + 1) or week == (mdt3 + 2) or week == (mdt3 + 3) or week == (mdt4 + 1) or week == (mdt4 + 2) or week == (mdt4 + 3) or week == (mdt5 + 1) or week == (mdt5 + 2) or week == (mdt5 + 3))): 
            eff_wow_decline = wow_decline
        
        else:
            eff_wow_decline = 1.00
            
        if(eff_wow_decline > 1):
            eff_wow_decline = 1.00
        

    
        if (week == 53):
            current_week = (week - 52)
            last_week = (week - 1)
    
        elif (week > 53): 
            current_week = (week - 52)
            last_week = (week - 53)
        
        else:
            current_week = (week)
            last_week = (week - 1)
        
        
        
        breakage_fact = min_value((1 - (final_breakage_map.get(float(rd(oh_prev,1)),1.00)) + (final_breakage_map.get(float(rd(oh_curr,1)),1.00))), 1.00)

        t = final_holiday_map.get(last_week,1.00)
        if t == 0.00:
		    t = 1.00    		
        if(season_cd in basics_list):
            wow = wow * breakage_fact * elas_factor  * csign  *(final_holiday_map.get(current_week,1.00) / t)*(final_exp_map.get(current_week,1.00)/final_exp_map.get(last_week,1.00))
        else:
            wow = wow * breakage_fact * elas_factor * eff_wow_decline * csign * seasonalitycoeff1 *(final_holiday_map.get(current_week,1.00) / t)*(final_exp_map.get(current_week,1.00)/final_exp_map.get(last_week,1.00))
        if float("%.2f" % df)==float("%.2f" % di) and week != mdt1:
                                ticketing=0

        if float("%.2f" % df)==float("%.2f" % di) and clearance_flag == 1:
            ticketing = 0 
        
        ticketingsum += ticketing*oh_curr

        units = min_value(wow,oh_curr)
        oh_prev=oh_curr 
        oh_curr = oh_curr - units   
        price = (1-df) * reg_price 
        revenue = price * units* ptd_ratio*((1-final_exprev_map.get(current_week,0.00)*0.01)*(final_expadj_map.get(current_week,0.00))+(1-final_expadj_map.get(current_week,0.00)))   
        tot_revenue += revenue
        di=df #Used to check for price change. 
        salvagefinal = salvage_value*oh_curr
        netrevenue = tot_revenue + salvagefinal - ticketingsum
        md0=int(math.ceil((discount * 100 )/ 5) * 5)
        if tempRevenue < netrevenue:
            tempRevenue=netrevenue
          
         
    arraymax = [key,md0,md_count,clearance_flag,md1,md2,md3,md4,md5,netrevenue,legit]    

    return(arraymax)

#Function to extract necessary necessary information for each key 
def mapping_fun(row):
    key = row[11]   
    Initial_Buy=float(row[14])
    week_dat = []
    OnHnd=float(row[7])
    discount=float(row[6])
    clearance_flag=int(row[21])
    Predicted_Units=float(row[8])
    ptd_ratio=float(row[20])
    salvage_value=float(row[16])
    ticketing=float(row[15])
    reg_price=float(row[5])
    egp_flag = int(row[22])
    md_count = int(row[10])
    oh_brekage_map_str = str(row[19])
    h_dow_fac_map_str = str(row[18])
    hd_fac_map_str = str(row[17])
    mdwk_map_str = str(row[9])
    elasticity = float(row[13])    
    seasonalitycoeff = float(row[23])
    circular_fact = float(row[24])
    cost = float(row[25])
    season_cd = str(row[26])
    plan_name = str(row[28])
    store_grp = str(row[29])
    org = str(row[30]).upper()
    exp_rev = str(row[33])
    exp_adj = str(row[32])
    exp = str(row[31])    
    lst = []
    
    brk = []
    oh = []
    
    hfac =[]
    dow =[]

    hd = []
    hdfac = []
    
    md = []
    mdwk = []
    
    oh_start = 0.10
    md1_max = 0
    
    
    #The following snippets extract the key-value pairs from the strings present in the assumption merge output 

    ################################High Discount##########################################
    hd_fac_split = hd_fac_map_str.split("hd")
    for j in hd_fac_split:
        x = (j[(len(j)-4):len(j)])
        hdfac.append(x)
    a1 = hdfac[0]
    hdfac.remove(a1)
    hdfac = [float(i) for i in hdfac]
        
    for i in range(15,95,5):
        hd.append(i)
    final_hd_map = dict(zip(hd,hdfac))
 
     
    ###################################################################################
    final_holiday_map = splitterv2("dow","F",h_dow_fac_map_str)
    
     
    ################################mdWeeks##########################################
    mdwk_split = mdwk_map_str.split("M")
    for j in mdwk_split:
        x = (j[(len(j)-2):len(j)])
        mdwk.append(x)
    a1 = mdwk[0]
    mdwk.remove(a1)
    mdwk = [int(i) for i in mdwk]
        
    for i in range(1,(md_count+1)):
        md.append(i)
    md.append(99)
    final_mdwk_map = dict(zip(md,mdwk))
    
    mdt1 = final_mdwk_map.get(1,1000)
    mdt2 = final_mdwk_map.get(2,1000)
    mdt3 = final_mdwk_map.get(3,1000)
    mdt4 = final_mdwk_map.get(4,1000)
    mdt5 = final_mdwk_map.get(5,1000)
    mos = final_mdwk_map.get(99,1000)


    final_breakage_map = splitterv2("OH","B",oh_brekage_map_str)
    final_exp_map = splitterv3("fwk","E",exp)
    final_expadj_map = splitterv3("fwk","Exp",exp_adj)
    final_exprev_map = splitterv3("fwk","Eof",exp_rev)

    md0=math.ceil((discount * 100 )/ 5.0) * 5
    md1_min = max_value(md0, 20)
    md1_max = md1_min + 20
    
#Discount guideline rules
    if org == 'comp2':
        #md1_max = 40
        md1_min = max_value(md0, 20)
        md1_max = md1_min + 20

        if egp_flag == 1 and clearance_flag == 0:
            md1_min = 15
            
        elif egp_flag == 0 and clearance_flag == 0:
			if md0 <=25:
				md1_min = 20
				md1_max = 40                
			elif md0>25 and md0 < 45:
				md1_min = 30
				md1_max = 50

			elif (md0 == 45):

				md1_min = 30
				md1_max = 55
							
			else:
				md1_min = 30
				md1_max = 60
        else:
			if md0>20:
				md1_max = md1_min + 15
			if md0>25:
				md1_max = md1_min + 10
			if md0>70:
				md1_max = md1_min + 5
			if md0>85:
				md1_max=90

		    
    elif org == 'comp1':
        if egp_flag == 1 and clearance_flag == 0:
            md1_min = max_value(md0, 10)
            md1_max = md1_min + 15

            if md0==5:
                md1_max = md1_min + 20
            if md0>=10:
                md1_max = md1_min + 25
            if md0==70:
                md1_max = md1_min + 20
            if md0>70:
                md1_max = md1_min + 5
            if md0>85:
                md1_max=90
        else:

            if md0>20:
                md1_max = md1_min + 15

            if md0>25:
                md1_max = md1_min + 10
            if md0>70:
                md1_max = md1_min + 5
            if md0>85:
                md1_max=90


    tempRevenue = 0.00
    tempst = 0.00
    arr_FinalCondmd1 = 0.00
    arr_FinalCondmd2 = 0.00
    arr_FinalCondmd3 = 0.00
    arr_FinalCondmd4 = 0.00
    arr_FinalCondmd5 = 0.00
    arr_FinalCondrev = 0.00
    arr_FinalCondoh = 0.00
    arr_Finalps = 0.00
    arr_FinalCondib = 0.00
    arr_FinalCondcd = 0.00
    arr_FinalCondsalvage = 0.00
    md2_max = 0.00
    md3_max = 0.00
    md4_max = 0.00
    md5_max = 0.00
    legit = 1    

####################################################################################


    def markdown_condition(md):
        if md<=40:
            return(md + 30)
        elif md<=50:
            return(md + 25)
        elif md<=60:
            return(md + 20)
        elif md<=75:
            return(md + 15)
        else:
            return(90)

        
    if md_count == 1:
#Legit is a flag to indicate the violation of clearance model discount guidelines        
        for md1 in range(max_value(int(md1_min-15),10),min_value(int(md1_max+20),95),5):
            legit = 1
            if md1 > md1_max or md1 < md1_min:
                legit = 0 
            lst.append(process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag,seasonalitycoeff,reg_price,md1,0,0,0,0,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit))
            
    elif md_count == 2:
        for md1 in range(max_value(int(md1_min-15),10),min_value(int(md1_max+20),95),5):
            md2_max = markdown_condition(md1)
            for md2 in range(md1,md2_max+5,5):
                legit = 1
                if md1 > md1_max or md1 < md1_min:
                    legit = 0
                lst.append(process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag,seasonalitycoeff,reg_price,md1,md2,0,0,0,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit))
                
    elif md_count == 3:
        for md1 in range(max_value(int(md1_min-15),10),min_value(int(md1_max+20),95),5):
            md2_max = markdown_condition(md1)
            for md2 in range(md1,md2_max+5,5):
                md3_max = markdown_condition(md2)
                for md3 in range(md2,md3_max+5,5):
                    legit = 1
                    if md1 > md1_max or md1 < md1_min:
                        legit = 0
                    lst.append(process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag,seasonalitycoeff,reg_price,md1,md2,md3,0,0,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit))
                   

    elif md_count == 4:
        for md1 in range(max_value(int(md1_min-15),10),min_value(int(md1_max+20),95),5):
            md2_max = markdown_condition(md1)
            for md2 in range(md1,md2_max+5,5):
                md3_max = markdown_condition(md2)
                for md3 in range(md2,md3_max+5,5):
                    md4_max = markdown_condition(md3)
                    for md4 in range(md3,md4_max+5,5):
                        legit = 1
                        if md1 > md1_max or md1 < md1_min:
                            legit = 0 
                        lst.append(process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag,seasonalitycoeff,reg_price,md1,md2,md3,md4,0,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit))
                        
                        
    elif md_count == 5:
        for md1 in range(max_value(int(md1_min-15),10),min_value(int(md1_max+20),95),5):
            md2_max = markdown_condition(md1)
            for md2 in range(md1,md2_max+5,5):
                md3_max = markdown_condition(md2)
                for md3 in range(md2,md3_max+5,5):
                    md4_max = markdown_condition(md3)
                    for md4 in range(md3,md4_max+5,5):
                        md5_max = markdown_condition(md4)
                        for md5 in range(md4,md5_max+5,5):
                            legit = 1
                            if md1 > md1_max or md1 < md1_min:
                                legit = 0
                            lst.append(process1(key,cost,salvage_value,ticketing,OnHnd,Initial_Buy,Predicted_Units,ptd_ratio,discount,clearance_flag,seasonalitycoeff,reg_price,md1,md2,md3,md4,md5,mdt1,mdt2,mdt3,mdt4,mdt5,mos,final_breakage_map,final_holiday_map,final_exp_map,final_expadj_map,final_exprev_map,final_hd_map,final_mdwk_map,elasticity,md_count,season_cd,plan_name,store_grp,org,oh_brekage_map_str,h_dow_fac_map_str,hd_fac_map_str,mdwk_map_str,exp_rev,exp_adj,exp,legit))
                  
                  


    return(lst)


final=AD_rdd.flatMap(lambda x : mapping_fun(x)) #Mapping function returns lst for each key. 


#Defining a schema
schema1=StructType([
        StructField("key",StringType(), True),
        StructField("md0",IntegerType(), True),
        StructField("md_count",IntegerType(), True),
        StructField("clearence",IntegerType(), True),
        StructField("md1",IntegerType(), True),
        StructField("md2",IntegerType(), True),
        StructField("md3",IntegerType(), True),
        StructField("md4",IntegerType(), True),
        StructField("md5",IntegerType(), True),
        
        StructField("net_rev",FloatType(), True),
        StructField("legit",IntegerType(), True),
            ])


df0  = HiveContext.createDataFrame(final, schema1)
df00 = df0.withColumn("delete_flag",F.when((df0.clearence == 1) & (df0.md0 <= 70) & ((df0.md1-df0.md0) == 5),1).otherwise(0))
df01 = df00.withColumn("delete_flag1",F.when((df00.md1 > 90)|(df00.md1 < 0) | (df00.md2 > 90) | ((df00.md3) > 90) | ((df00.md4) > 90) | ((df00.md5) > 90),1).otherwise(0))
df = df01.filter("delete_flag = 0 and delete_flag1 =0")

w = Window.partitionBy("key","legit").orderBy(desc("net_rev"),asc("md1"),
                                     asc("md2"),asc("md3"),
                                     asc("md4"),asc("md4"),asc("md5"))


udf1 = udf(splitstring,ArrayType(IntegerType()))



#Finding the optimal values for a discount combination
df1 = df
df1 = df1.withColumn("rev_rank", rank().over(w))
df1 = df1.withColumn("maxcomb", F.when((df1.rev_rank == 1) & (df1.legit == 1),1).otherwise(0)).repartition("key")

df_maxcomb = df1.filter("maxcomb = 1") 
df_maxcomb = df_maxcomb.select(df_maxcomb.key.alias("KEY1"),
                               df_maxcomb.md1.alias("md1_maxcomb"),
                               df_maxcomb.md2.alias("md2_maxcomb"),
                               df_maxcomb.md3.alias("md3_maxcomb"),
                               df_maxcomb.md4.alias("md4_maxcomb"),
                               df_maxcomb.md5.alias("md5_maxcomb")).repartition("KEY1")                            


final_df = df1.join(df_maxcomb,(df1.key==df_maxcomb.KEY1),"left")                               
    
final_df = final_df.withColumn("md1_depth",(final_df.md1 - final_df.md1_maxcomb))                   .withColumn("md2_depth",(final_df.md2 - final_df.md2_maxcomb))                   .withColumn("md3_depth",(final_df.md3 - final_df.md3_maxcomb))                   .withColumn("md4_depth",(final_df.md4 - final_df.md4_maxcomb))                   .withColumn("md5_depth",(final_df.md5 - final_df.md5_maxcomb))
final_df_all = final_df.filter("md1_depth = -5 or md1_depth = -10 or md1_depth = -15 or md1_depth = 0 or md1_depth = 5 or md1_depth = 10 or md1_depth = 15 ")

#Determining Scenarios
final_df_all1 = final_df_all.withColumn("scenario1", F.when(((final_df_all.md2_depth == 0)&(final_df_all.md3_depth == 0)&(final_df_all.md4_depth == 0) & (final_df_all.md1_depth != 0) & (final_df_all.md5_depth == 0)),1).otherwise(0))
final_df_all2 = final_df_all1.withColumn("scenario2",F.when((( (final_df_all1.md_count == 1) | ((final_df_all1.md_count == 2  ) & (final_df_all1.md1_depth==final_df_all1.md2_depth)) | ((final_df_all1.md_count == 3  ) & (final_df_all1.md1_depth==final_df_all1.md2_depth) & (final_df_all1.md2_depth==final_df_all1.md3_depth))  | ((final_df_all1.md_count == 4  ) & (final_df_all1.md1_depth==final_df_all1.md2_depth) & (final_df_all1.md2_depth==final_df_all1.md3_depth) & (final_df_all1.md2_depth==final_df_all1.md4_depth)) | ((final_df_all1.md_count == 5  ) & (final_df_all1.md1_depth==final_df_all1.md2_depth) & (final_df_all1.md2_depth==final_df_all1.md3_depth) & (final_df_all1.md2_depth==final_df_all1.md4_depth) & (final_df_all1.md2_depth==final_df_all1.md5_depth))) & (final_df_all1.md1_depth !=0)) ,1).otherwise(0))
w1 = Window.partitionBy("key","md1").orderBy(desc("net_rev"))
final_df_all3 = final_df_all2.withColumn("scenario3",F.when(((row_number().over(w1) == 1) & (final_df_all2.md1_depth != 0 )),1).otherwise(0)).repartition("key") 


#Modifying scenario-2
def relavance_scorer(md1_depth,md2_depth,md3_depth,md4_depth,md5_depth,maxsin2):
    relavance = 0
    if(maxsin2==0):
        if(md1_depth == md2_depth and md2_depth !=0 and md3_depth == 0 and md4_depth == 0 and md5_depth == 0):
            relavance = 1
        if(md1_depth == md2_depth and md2_depth == md3_depth and md1_depth!=0 and md4_depth == 0 and md5_depth == 0):
            relavance = 2
        if(md1_depth == md2_depth and md2_depth == md3_depth and md1_depth!=0 and md4_depth == md1_depth and md5_depth == 0):
            relavance = 3
            
    return(relavance)

udf2 = udf(relavance_scorer,(IntegerType()))

def scenario_2_updater(scenario2,scenario2_new):
    if(scenario2 == 1 or scenario2_new == 1):
        scenario_updated = 1
    else:
        scenario_updated = 0
    return (scenario_updated)
scenario_update = udf(scenario_2_updater,(IntegerType())) 

windowSpec = Window.partitionBy(final_df_all3['key'],final_df_all3['md1_depth']) 
final_df_all4 = final_df_all3.withColumn("maxsin2",max(final_df_all3.scenario2).over(windowSpec))
final_df_all4 = final_df_all4.withColumn("relavance",udf2(final_df_all4.md1_depth,final_df_all4.md2_depth,final_df_all4.md3_depth,final_df_all4.md4_depth,final_df_all4.md5_depth,final_df_all4.maxsin2))

w1 = Window.partitionBy("key","md1_depth").orderBy(desc("relavance"))

final_df_all5 = final_df_all4.withColumn("scenario2_new",F.when((row_number().over(w1) == 1) & (final_df_all4.relavance != 0 ) ,1).otherwise(0))
final_df_all6 = final_df_all5.withColumn("scenario2_updt",scenario_update(final_df_all5.scenario2,final_df_all5.scenario2_new))
final_df_all6 = final_df_all6.withColumnRenamed("scenario2","scenario2_old")
final_df_all6 = final_df_all6.withColumnRenamed("scenario2_updt","scenario2")
final_df_all6.write.mode("overwrite").saveAsTable('opt_sin_table_2')


