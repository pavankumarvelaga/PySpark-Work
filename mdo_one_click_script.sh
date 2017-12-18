a=`hive -e "select count(*) from aff_comp1_comp2_mdo_input where md_count < 6"`
if [ $a -gt 0 ]
then
    echo " Now running commands";
    rm /staging/skprccm/aff_mdo_comp1_comp2_input.csv
    gsutil mv gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv gs://affine_poc/skprccm/input_repository/`date +%Y-%m-%d`/aff_mdo_comp1_comp2_input.csv
    hive -e "set hive.cli.print.header=True;  select * from (select *,row_number() over(partition by key order by run_date desc) as a from aff_comp1_comp2_mdo_input) as temp_table where temp_table.a = 1" | sed 's/,/-/g' | sed 's/temp_table.//g' >> /staging/skprccm/aff_mdo_comp1_comp2_input.csv
    gsutil cp /staging/skprccm/aff_mdo_comp1_comp2_input.csv gs://affine_poc/skprccm/aff_mdo_comp1_comp2_input.csv
    gcloud dataproc clusters create temp-cluster --master-machine-type=n1-standard-8 --worker-machine-type=n1-highmem-32 --num-workers=12
    gcloud dataproc jobs submit pyspark --cluster temp-cluster gs://affine_poc/skprccm/automation_script/mdo_master_code_error_handled_vf.py
    gcloud dataproc jobs submit pyspark --cluster temp-cluster gs://affine_poc/skprccm/automation_script/ui_update_script_new_1.py
    yes Y | gcloud dataproc clusters delete temp-cluster
	#hive -e "truncate table if exists aff_comp1_comp2_mdo_input"
else
    echo "There are no rows, hence the code will not run";
fi

