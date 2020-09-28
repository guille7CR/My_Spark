from subprocess import PIPE , Popen
import subprocess
import os
import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession

 


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
        
#=======================================
        
# Extracting require table

 
sbu_norm = spark.sql('Select n.pbin, n.trn_ts as date,  n.table_nm,  n.column_nm,  n.after_value,  n.before_value,  n.sbu_user_id,  n.sbu_supplier_nb From edf_bis_dd_en.da_sbu_normalized n left join edf_bis_dd_en.da_sbu_transformed t on n.pbin=t.pbin and n.trn_ts=t.trn_ts and n.table_nm=t.table_nm and n.column_nm=t.column_nm where t.trn_ts IS NULL')

sbu_norm.show()

#=======================================

#Count total of records before transformation

#sbu_norm.count()

#=======================================

#Group By each value present in column before transformation

#sbu_norm.groupBy('column_nm').count().show()

#=======================================


# Removing leading and trailing space in "column_nm" column

sbu_norm = sbu_norm.withColumn('column_nm', trim(sbu_norm.column_nm))

#=======================================

# Updating strings values

sbu_norm = sbu_norm.withColumn('date', sbu_norm['date'].substr(0, 10)).withColumn("column_nm", when(col("column_nm") == "LGL_NM", "Name").when(col("column_nm") == "PRI_AIN", "AIN").when(col("column_nm") == "SIC_CD_PRI", "SIC").when(col("column_nm") == "ACT_EMP_SZ", "Emp_Size").when(col("column_nm") == "ACT_SLS_SZ", "Sls_Size").when(col("column_nm") == "PRI_PH_NB_PFX", "Phone_PFX").when(col("column_nm") == "PRI_PH_NB_SFX", "Phone_SFX").when(col("column_nm") == "PRI_PH_AREA", "Phone_Area").when(col("column_nm") == "CHILD_PBIN", "CHILD_PBIN").when(col("column_nm") == "PARENT_PBIN", "PARENT_PBIN").when(col("column_nm") == "PBIN", "PBIN").when(col("column_nm") == "HQ_BRANCH_CD", "HQ_BRANCH_CD").otherwise("1"))

#=======================================

#Filter by Name to verify if this value is present in dataset

#sbu_norm.filter(sbu_norm.column_nm == "Name").show(truncate=False)

#=======================================

#Count total of records after operation

#sbu_norm.count()

#=======================================

#Group By each value present in resultant column

#sbu_norm.groupBy('column_nm').count().show()

#===============================================================

#Filter DATASET by everything but JBROWN AND 1

 
sbu_norm = sbu_norm.filter(sbu_norm.sbu_user_id != "JBROWN").filter(sbu_norm.column_nm != "1")

sbu_normL = sbu_norm.filter(sbu_norm.sbu_user_id != "JBROWN").filter(sbu_norm.column_nm != "1")

sbu_norm = sbu_norm.withColumnRenamed("pbin", "R_pbin").withColumnRenamed('column_nm','R_column_nm').withColumnRenamed('date','R_date')

#===============================================================

#Group By PBIN, DATE, COLUMN NM and agg by Max(after, before) values

sbu_norm= sbu_norm.groupBy('R_pbin','R_date', 'R_column_nm').agg(F.max('after_value'), F.max('before_value'))

sbu_norm.show()

#sbu_norm.count()

#===============================================================

# Creating Aliases for easier management

L = sbu_normL.alias('L')

R = sbu_norm.alias('R')


# Joining Tables

df_normalized = L.join(R, (L.pbin == R.R_pbin) & (L.date == R.R_date) & (L.column_nm == R.R_column_nm))


#===============================================================


#Adding additional firmographic elements as null values

#df_normalized = df_normalized.withColumn('issue_type',lit('null'))\
    #.withColumn('bin',lit('null'))\
    #.withColumn('lvid',lit('null'))\
    #.withColumn('bus_nm',lit('null'))\
    #.withColumn('ain',lit('null'))\
    #.withColumn('contact',lit('null'))\
    #.withColumn('description',lit('null'))\


#df_normalized.show()

#===============================================================

#Removing extra columns 

columns_to_drop = [ 'after_value', 'before_value', 'R_pbin','after_value','before_value','R_date','R_column_nm']


df_normalized = df_normalized.drop(*columns_to_drop)


df_normalized.show()

#===============================================================

# Rename max after and max before

df_normalized = df_normalized.withColumnRenamed("max(before_value)", "before").withColumnRenamed('max(after_value)','after').withColumnRenamed('date','trn_ts')


#===============================================================

#Filter by after = before

df_normalized = df_normalized.filter(trim(df_normalized.before) != trim(df_normalized.after))

df_normalized.show()

df_normalized.dtypes

#===============================================================
#Rearrange columns

df_normalized= df_normalized.select("trn_ts","pbin","column_nm","table_nm", "after","before","sbu_user_id","sbu_supplier_nb")

df_normalized.show()

#=======================================

# Saving Data

df_normalized.write.mode("append").insertInto('edf_bis_dd_en.da_sbu_transformed')

spark.sql('SELECT * FROM edf_bis_dd_en.da_sbu_transformed').show()