import os
import sys 
import subprocess 
#Transfering codes
os.system("gsutil cp gs://affine_poc/skprccm/finalized_codes_mdo/*.py ./")
#Batching the input data
a=os.system("spark-submit batcher.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")

if a>0:
   print("Batcher Failed")
   sys.exit(a)
    
subprocess.call(['hive -e "show tables;" > ./table_names.txt'], shell=True)
txt = open("table_names.txt")
a=txt.read()
#Finding the names of the newly batched tables
list_of_tables = list(a.split("\n"))
list_of_tables.remove(list_of_tables[len(list_of_tables)-1])
print(list_of_tables)
for j in list_of_tables:
   print(j)
   cmd1 = "spark-submit part_one.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0 " + str(j)
   cmd2 = "spark-submit part_two.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0 " + str(j)
   b = subprocess.call([cmd1],shell=True)
   if b>0:
      print("Scenario generator (Part one) failed")
      sys.exit(b)
   c = subprocess.call([cmd2],shell=True)
   if c>0:
      print("Metric calculator (Part two) failed")
      sys.exit(c)

#Running the rest of the codes   

d=os.system("spark-submit imputation.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")
e=os.system("spark-submit extraction.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")
if d>0:
   print("imputation failed")
   sys.exit(d)

if(e>0):
   print("extraction failed")
   sys.exit(e)
os.system("spark-submit current_metrics.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")
os.system("spark-submit week_agg.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")
os.system("spark-submit md_agg.py --properties spark.jars.packages=com.databricks:spark-csv_2.11:1.2.0")
subprocess.call(['hive -e \" select * from final_transposed_data where md_perc is not null\" | sed \'s/[\t]/,/g\' >> ./md_level_data.txt'], shell=True)
subprocess.call(['hive -e \" select * from week_agg_final_x\" | sed \'s/[\t]/,/g\' >> ./week_level_data.txt'], shell=True)
subprocess.call(['hive -e \" select * from current_metrics_final_x\" | sed \'s/[\t]/,/g\' >> ./current_metrics.txt'], shell=True)
os.system("gsutil cp ./md_level_data.txt gs://affine_poc/dbinputfiles/`date +%Y-%m-%d`/aff_mdo_simulation_md_level_metrics.txt")
os.system("gsutil cp ./week_level_data.txt gs://affine_poc/dbinputfiles/`date +%Y-%m-%d`/aff_mdo_simulation_week_level_metrics.txt")
os.system("gsutil cp ./current_metrics.txt gs://affine_poc/dbinputfiles/`date +%Y-%m-%d`/aff_mdo_simulation_current_metrics.txt")

