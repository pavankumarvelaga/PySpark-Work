#Importing libraries
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


#Defining a function to round a number
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

#Function to extract key value pairs from a string
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




final_1 = HiveContext.sql("select * from opt_sin_table_2").filter("scenario1 = 1 or scenario2 = 1 or scenario3 = 1 or maxcomb = 1")


final_1 = final_1.filter("scenario1 = 1 or scenario2 = 1 or scenario3 = 1 or maxcomb = 1")

final2 = final_1.select("key","md1","md2","md3","md4","md5","scenario1","scenario2","scenario3","maxcomb","md1_depth","legit").repartition("key")


temp = len(sys.argv)
file_name = "select * from " + sys.argv[temp-1]
ref = HiveContext.sql(file_name)


#Performing a join to obtain relavant information for each key
ref1 = ref.select(ref.key.alias("key2"),"reg_price","disc","on_hand","predicted_sale","md_week","md_count","initial_pe","initial_buy","sub","sal_value","high_disc_fact","h_dow_fac","oh_breakage","ptd_ratio","clearance_flag_final","seasonality","cost","season_cd","h4","plan_name","store_grp","org","fwk_extra_percent_off","fwk_extra_pc_off_adj","fwk_extra_pc_off","wow_decline").repartition("key2")

final_df_all5_1 = final2.join(ref1,ref1.key2 == final2.key)

final_df_all6 = final_df_all5_1.select("wow_decline","reg_price","disc","on_hand","predicted_sale","md_week","md_count","key2","initial_pe",coalesce(final_df_all5_1.initial_buy.cast("float"),lit(0)).alias("initial_buy"),"sub","sal_value","high_disc_fact","h_dow_fac","oh_breakage","ptd_ratio","clearance_flag_final","seasonality",coalesce(final_df_all5_1.cost.cast("float"),final_df_all5_1.reg_price * 0.25).alias("cost"),"season_cd","h4","plan_name","store_grp","org","fwk_extra_percent_off","fwk_extra_pc_off_adj","fwk_extra_pc_off","legit","md1","md2","md3","md4","md5","md1_depth","maxcomb","scenario1","scenario2","scenario3")

temp_rdd=final_df_all6.rdd

#Function to calculate metrics for each key
def process_2(row):
    
    key = row[7]   
    Initial_Buy=float(row[9])
    week_dat = []
    OnHnd=float(row[3])
    
    discount=float(row[2])
    clearance_flag=int(row[16])
    Predicted_Units=float(row[4])
    ptd_ratio=float(row[15])
    
    salvage_value=float(row[11])
    ticketing=float(row[10])
    reg_price=float(row[1])
    md_count = int(row[6])
    oh_brekage_map_str = str(row[14]) 
    h_dow_fac_map_str = str(row[13])
    hd_fac_map_str = str(row[12])
    mdwk_map_str = str(row[5])
    elasticity = float(row[8])
    
    seasonalitycoeff = float(row[17])
    cost = float(row[18])
    season_cd = str(row[19])
    plan_name = str(row[21])
    store_grp = str(row[22])
    org = str(row[23])
    exp_rev = str(row[26])
    exp_adj = str(row[25])
    exp = str(row[24])
    legit_flag = int(row[27])
    md1 = int(row[28])
    md2 = int(row[29])
    md3 = int(row[30])
    md4 = int(row[31])
    md5 = int(row[32])
    md1_depth = int(row[33])
    maxcomb = int(row[34])
    scenario1 = int(row[35])
    scenario2 = int(row[36])
    scenario3 = int(row[37])
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
    md1_avg_units = 0.0
    md2_avg_units = 0.0
    md3_avg_units = 0.0
    md4_avg_units = 0.0
    md5_avg_units = 0.0
    
    
 
    ################################Holiday##########################################
    holiday_fac_split = h_dow_fac_map_str.split("dow")
    for j in holiday_fac_split:
        x = (j[(len(j)-4):len(j)])
        hfac.append(x)
    a1 = hfac[0]
    hfac.remove(a1)
    hfac = [float(i) for i in hfac]
        
    for i in range(1,53):
        dow.append(i)
    #final_holiday_map = dict(zip(dow,hfac))
    final_holiday_map = splitterv2("dow","F",h_dow_fac_map_str)
    #################################################################################
 
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
    arr_FinalCondst = ""
            

    md_count = md_count
    tempRevenue = 0.00
    tempst = 0.00
    wk_dict = {}
    wk_lst =[]

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
    dummy1 = 0.00
    dummy2 = 0.00
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
    md1_rev = 0.00
    md2_rev = 0.00
    md3_rev = 0.00
    md4_rev = 0.00
    md5_rev = 0.00
    md1_cmd = 0.00
    md2_cmd = 0.00
    md3_cmd = 0.00
    md4_cmd = 0.00
    md5_cmd = 0.00
    md1_st  = 0.00
    md2_st = 0.00
    md3_st = 0.00
    md4_st = 0.00
    md5_st = 0.00
    md1_oh = 0.00
    md2_oh = 0.00
    md3_oh = 0.00
    md4_oh = 0.00
    md5_oh = 0.00
    md1_cost = 0.0
    md2_cost = 0.0
    md3_cost = 0.0
    md4_cost = 0.0
    md5_cost = 0.0
    md1_gm = 0.0
    md2_gm = 0.0
    md3_gm = 0.0
    md4_gm = 0.0
    md5_gm = 0.0
    md1_gma = 0.0
    md2_gma = 0.0
    md3_gma = 0.0
    md4_gma = 0.0
    md5_gma = 0.0
    gma_final = 0.0
    tot_cost = 0.0
    tot_units = 0.00
    md1_price = 0.0
    md2_price = 0.0
    md3_price = 0.0
    md4_price = 0.0
    md5_price = 0.0
    wos = 0.0
    per_margin = 0.0
    md1_wos = 0.0
    md2_wos = 0.0
    md3_wos = 0.0
    md4_wos = 0.0
    md5_wos = 0.0
    cr_cost = 0.0
    decline = 0.45
    wow_decline = 1.00
    strMaster = 0.00
    
    price_overall = 0.00
    md1_units = 0.00
    md2_units = 0.00
    md3_units = 0.00
    md4_units = 0.00
    md5_units = 0.00
    tot_units = 0.00
    week_length = 0.00
    dfk = 0
    checkout_rate = 0.0


    arr_FinalCondst = ""
    di = discount
    wow = Predicted_Units
    week_table = []
    oh_curr = OnHnd - Predicted_Units
    oh_prev = OnHnd
    week = mdt1
    current_week = str(week)
    last_week = str(week)
    md_no = 0
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


        if (week == 48):
            seasonalitycoeff1 = seasonality48
        elif (week == 49):
            seasonalitycoeff1 = seasonality49

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

        if (week==mdt1 and clearance_flag == 0):
            cmd = reg_price * (-1) * df * oh_curr
        else:
            cmd = reg_price * (di - df ) * oh_curr


        units = min_value(wow,oh_curr)
        if units > 0.0:
            wos = oh_curr/units
        else:
            wos = 0.0

        oh_prev=oh_curr 
        oh_curr = oh_curr - units    
        price = reg_price * (1 - df )  
        revenue = price * units* ptd_ratio*((1-final_exprev_map.get(current_week,0.00)*0.01)*(final_expadj_map.get(current_week,0.00))+(1-final_expadj_map.get(current_week,0.00)))   
        cr_cost = cost * units
        tot_revenue += revenue
        tot_cost = tot_cost + cr_cost
        tot_units = tot_units+units
        week_margin = revenue - cr_cost
        if revenue > 0:
            per_margin = week_margin/revenue
        else :
            per_margin = 0.0
        tot_margin = tot_margin+week_margin
        
        #Initial discount should be set to last ongoing discount 

        di=df #Used to check for price change. 
        salvagefinal = salvage_value*oh_curr
        netrevenue = tot_revenue + salvagefinal - ticketingsum
        if(oh_prev>0):
            checkout_rate = units/oh_prev
        else:
            checkout_rate = 0.0 
        #md values calculation
        if int(week)>=mdt1 and int(week) < mdt2:
            
            md1_rev = md1_rev+revenue
            md1_cmd = md1_cmd + cmd
            md1_oh = oh_curr
            md1_cost = md1_cost + cr_cost
            if(wow > 0):
                md1_wos = oh_curr/wow
            else:
                md1_wos = 0.0
                

            md1_gma = md1_rev-md1_cost
            md1_price = price
            md1_units  = md1_units +  units

        elif int(week)>=mdt2 and int(week) <mdt3:
            
            
            md2_rev = md2_rev+revenue
            md2_cmd = md2_cmd + cmd
            md2_oh = oh_curr
            md2_cost = md2_cost + cr_cost
            if(wow > 0):
                md2_wos = oh_curr/wow
            else:
                md2_wos = 0.0
                
            md2_gma = md2_rev-md2_cost
            md2_price = price
            md2_units  = md2_units +  units

        elif int(week)>=mdt3 and int(week) <mdt4:
            md3_rev = md3_rev+revenue
            md3_cmd = md3_cmd + cmd
            md3_oh = oh_curr
            md3_cost = md3_cost + cr_cost
            if(wow > 0):
                md3_wos = oh_curr/wow
            else:
                md3_wos = 0.0
            
            md3_gma = md3_rev-md3_cost
            md3_price = price
            md3_units  = md3_units +  units

        elif int(week)>=mdt4 and int(week) <mdt5:
            md4_rev = md4_rev+revenue

            md4_cmd = md4_cmd + cmd
            md4_oh = oh_curr
            md4_cost = md4_cost + cr_cost
            if(wow > 0):
                md4_wos = oh_curr/wow
            else:
                md4_wos = 0.0
            
            md4_gma = md4_rev-md4_cost
            md4_price = price
            md4_units  = md4_units +  units

        elif int(week)>=mdt5 and int(week) <= mos:
            md5_rev = md5_rev+revenue

            md5_cmd = md5_cmd + cmd
            md5_oh = oh_curr
            md5_cost = md5_cost + cr_cost
            if(wow > 0):
                md5_wos = oh_curr/wow
            else:
                md5_wos = 0.0
            
            md5_gma = md5_rev-md5_cost
            md5_price = price
            md5_units  = md5_units +  units


        
        
        
        if tempRevenue < netrevenue:
            tempRevenue=netrevenue
#Writing week level data        
        wk_lst.append([float(week),float(revenue),float(week_margin),float(cmd),float(oh_curr),float(per_margin),float(cr_cost),float(units)])

    netrevenue = tot_revenue + salvagefinal - ticketingsum
    arr_FinalCondmd1 = md1
    arr_FinalCondmd2 = md2
    arr_FinalCondmd3 = md3
    arr_FinalCondmd4 = md4
    arr_FinalCondmd5 = md5
    arr_FinalCondib = Initial_Buy
    arr_FinalCondcd = discount
    arr_Finalps = Predicted_Units
    arr_FinalCondoh = oh_curr
    arr_FinalCondrev = tempRevenue
    arr_FinalCondsalvage = salvagefinal
    salvage_cost = oh_curr * cost
    tot_cost = tot_cost + salvage_cost
    gma_final = netrevenue-tot_cost
    md0=int(math.ceil((discount * 100 )/ 5) * 5)
    md_count = md_count
    #arraymax is a list which contains a single discount combination for a single key. 
    if md_count == 1:
        md2_oh = md1_oh
        md3_oh = md1_oh
        md4_oh = md1_oh
        md5_oh = md1_oh
    elif md_count == 2:
        md3_oh = md2_oh
        md4_oh = md2_oh
        md5_oh = md2_oh
    elif md_count == 3:
        md4_oh = md3_oh
        md5_oh = md3_oh
        
    elif md_count == 4:
        md5_oh = md4_oh
        
    
    arraymax = [key,md0,md_count,clearance_flag,arr_FinalCondmd1,arr_FinalCondmd2,arr_FinalCondmd3,arr_FinalCondmd4,
                        arr_FinalCondmd5,reg_price,arr_FinalCondrev,arr_Finalps,arr_FinalCondoh,
                       arr_FinalCondib,arr_FinalCondcd,arr_FinalCondsalvage,
                    md1_rev,md2_rev,md3_rev,md4_rev,md5_rev,
                    md1_cmd,md2_cmd,md3_cmd,md4_cmd,md5_cmd,
                   md1_oh,md2_oh,md3_oh,md4_oh,md5_oh,
                mdt1,mdt2,mdt3,mdt4,mdt5,mos,tot_cost,md1_cost,md2_cost,md3_cost,md4_cost,md5_cost,md1_price,md2_price,md3_price,md4_price,md5_price,md1_wos,md2_wos,md3_wos,md4_wos,md5_wos,tot_margin,md1_units,md2_units,md3_units,md4_units,md5_units,md1_gma,md2_gma,md3_gma,md4_gma,md5_gma,gma_final,season_cd,plan_name,store_grp,org,legit_flag,wk_lst,md1_depth,maxcomb,scenario1,scenario2,scenario3
                 ]    

    return(arraymax)        



final_temp=temp_rdd.map(lambda x : process_2(x))




schema3=StructType([
        StructField("key",StringType(), True),
        StructField("md0",IntegerType(), True),
        StructField("md_count",IntegerType(), True),
        StructField("clearence",IntegerType(), True),
        StructField("MD1",IntegerType(), True),
        StructField("MD2",IntegerType(), True),
        StructField("MD3",IntegerType(), True),
        StructField("MD4",IntegerType(), True),
        StructField("MD5",IntegerType(), True),
        StructField("reg_price",FloatType(), True),
        StructField("net_rev",FloatType(), True),

        StructField("predicted_sales",FloatType(), True),
        StructField("final_oh",FloatType(), True),
        StructField("initial_buy",FloatType(), True),
        StructField("current_disc",FloatType(), True),
        StructField("salvage",FloatType(), True),
        StructField("md1_rev",FloatType(), True),
        StructField("md2_rev",FloatType(), True),
        StructField("md3_rev",FloatType(), True),
        StructField("md4_rev",FloatType(), True),
        StructField("md5_rev",FloatType(), True),
        StructField("md1_cmd",FloatType(), True),
        StructField("md2_cmd",FloatType(), True),
        StructField("md3_cmd",FloatType(), True),
        StructField("md4_cmd",FloatType(), True),
        StructField("md5_cmd",FloatType(), True),
        StructField("md1_oh",FloatType(), True),
        StructField("md2_oh",FloatType(), True),
        StructField("md3_oh",FloatType(), True),
        StructField("md4_oh",FloatType(), True),
        StructField("md5_oh",FloatType(), True),
        StructField("mdt1",IntegerType(), True),
        StructField("mdt2",IntegerType(), True),
        StructField("mdt3",IntegerType(), True),
        StructField("mdt4",IntegerType(), True),
        StructField("mdt5",IntegerType(), True),
        StructField("mos",IntegerType(), True),
        
        StructField("tot_cost",FloatType(), True),
        StructField("md1_cost",FloatType(), True),
        StructField("md2_cost",FloatType(), True),
        StructField("md3_cost",FloatType(), True),
        StructField("md4_cost",FloatType(), True),
        StructField("md5_cost",FloatType(), True),
        StructField("md1_price",FloatType(), True),
        StructField("md2_price",FloatType(), True),
        StructField("md3_price",FloatType(), True),
        StructField("md4_price",FloatType(), True),
        StructField("md5_price",FloatType(), True),
        #StructField("overall_price",FloatType(), True),
        StructField("md1_wos",FloatType(), True),
        StructField("md2_wos",FloatType(), True),
        StructField("md3_wos",FloatType(), True),
        StructField("md4_wos",FloatType(), True),
        StructField("md5_wos",FloatType(), True),
        StructField("tot_margin",FloatType(), True),
        StructField("md1_units",FloatType(), True),
        StructField("md2_units",FloatType(), True),
        StructField("md3_units",FloatType(), True),
        StructField("md4_units",FloatType(), True),
        StructField("md5_units",FloatType(), True),
        StructField("md1_gma",FloatType(), True),
        StructField("md2_gma",FloatType(), True),
        StructField("md3_gma",FloatType(), True),
        StructField("md4_gma",FloatType(), True),
        StructField("md5_gma",FloatType(), True),
        StructField("gma_final",FloatType(), True), 
        StructField("season_cd",StringType(), True),
        StructField("plan_name",StringType(), True),
        StructField("store_grp",StringType(), True),
        StructField("org",StringType(), True),
        StructField("legit_flag",IntegerType(), True),
        StructField("weeklevel_data",ArrayType(ArrayType(FloatType())), True),
        StructField("md1_depth",IntegerType(), True),
        StructField("maxcomb",IntegerType(), True),
        StructField("scenario1",IntegerType(), True),
        StructField("scenario2",IntegerType(), True),
        StructField("scenario3",IntegerType(), True),
        
    ])
    


dftry  = HiveContext.createDataFrame(final_temp, schema3)
dftry.write.mode("append").saveAsTable('final_one_1')

