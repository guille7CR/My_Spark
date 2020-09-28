

From pyspark.sql.session import SparkSession
From subprocess import PIPE , Popen
Import subprocess
Import os
Import Numpy as np
Import Pandas as pd

spark = SparkSession \
        .builder \
        .appName ( "My App Testing" ) \
        .master ( "yarn" ) \
        .config ( "spark.dynamicAllocation.enabled" , "true" ) \
        .config ( "spark.executor.cores" , "4" ) \
        .config ( "spark.executor.memory" , "10g" ) \
        .config ( "spark.executor.memoryOverhead" , "3g" ) \
        .config ( "spark.driver.cores" , "4" ) \
        .config ( "spark.driver.memory" , "20g" ) \
        .config ( "spark.driver.memoryOverhead" , "2g" ) \
        .config ( "spark.sql.execution.arrow.enabled" , "false" ) \
        .config ( "spark.shuffle.service.enabled" , "true" ) \
        .config ( "spark.dynamicAllocation.initialExecutors" , "1" ) \
        .config ( "spark.dynamicAllocation.minExecutors" , "1" ) \
        .config ( "spark.dynamicAllocation.maxExecutors" , "10" ) \
        .config ( "spark.yarn.queue" , "root.bis.std.edf_bis_dd_de" ) \
        .enableHiveSupport() \
        .getOrCreate()
        
# Extracting required tables 

sbu_table = spark.sql('SELECT * FROM edf_bis_dd_en.sbu_table')
event_log = spark.sql('SELECT DISTINCT n.trn_ts,n.family_pbin as pbin, sbu_table_id, sbu_column_id, n.sbu_supplier_nb, n.sbu_user_id, n.before_value, n.after_value, n.suprs_type_cd FROM edf_bis_dd_en.sbu_event_log n LEFT JOIN edf_bis_dd_en.da_sbu_normalized t on n.family_pbin=t.pbin and n.trn_ts=t.trn_ts where t.trn_ts IS NULL') 
sbu_column = spark.sql('select * from edf_bis_dd_en.sbu_column')

 
# Creating Aliases

st = sbu_table.alias('st')
el = event_log.alias('el')
sc = sbu_column.alias('sc')

# Joining Tables

df_normalized = st.join(el, st.sbu_table_id == el.sbu_table_id,'inner').join(sc, el.sbu_table_id == sc.sbu_table_id, 'inner')
df_normalized = df_normalized.drop('sbu_table_id','sbu_column_id','L_R_sbu_table_id','derived_in','create_db_dt','upd_db_dt','upd_db_id','R_sbu_table_id','R_sbu_column_id','column_datatype_id','R_create_db_dt','R_upd_db_dt','R_upd_db_id')
df_normalized.show()

# Saving Data

df_normalized.write.mode("overwrite").insertInto('edf_bis_dd_en.da_sbu_normalized')

spark.sql('SELECT * FROM edf_bis_dd_en.da_sbu_normalized').show()
