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
        
# Extracting require tables

 
sbu_trans = spark.sql('Select * From edf_bis_dd_en.da_sbu_transformed')

firmo_sup= spark.sql('Select * From edf_bis_dd_en.da_sbu_firmo_sup')

addr = spark.sql('''

Select pbin, child_pbin, lvid, std_usps_nprf_str, std_city_nm, 
std_state_cd, std_zip5_cd, ain, name_norm_long, ol_lgl_nm_long 
From edf_bis_dd_en.da_firmo

''')

sbu_trans.show()

firmo_sup.show()

addr.show()

#=======================================  SBU_TRANS =======================

# Removing leading and trailing space in "column_nm" column

sbu_trans = sbu_trans.withColumn('column_nm', trim(sbu_trans.column_nm))

# Adding new structure column and populate it  with 1 or 0

sbu_trans = sbu_trans.withColumn("structure", when(col("column_nm") == "CHILD_PBIN", "1").when(col("column_nm") == "PARENT_PBIN", "1").when(col("column_nm") == "PBIN", "1").when(col("column_nm") == "HQ_BRANCH_CD", "1").otherwise("0"))


#Renaming columns in SBU TRANSFORMED

sbu_trans= sbu_trans.withColumnRenamed("after", "after_value").withColumnRenamed("before","before_value")


#Filtering out as per structure value

sbu_trans = sbu_trans.filter(sbu_trans.structure == "0")

sbu_trans.show()

sbu_trans.count()


# Filtering UNIQUE VALUES

sbu_trans= sbu_trans.dropDuplicates()

sbu_trans.count()


# Removing trailing zeros from left -- for after and before values

sbu_trans = sbu_trans.withColumn('after_value', when(col('after_value') > 1, F.regexp_replace('after_value', r'^[0]*', '')).otherwise(sbu_trans.after_value))

sbu_trans = sbu_trans.withColumn('before_value', when(col('before_value') > 1, F.regexp_replace('before_value', r'^[0]*', '')).otherwise(sbu_trans.before_value))

sbu_trans.show()


# ============================STAGE COMPLETED FOR SBU =====================




#=======================================  FIRMO ===========================

# Removing leading and trailing space in "DT_FIRST_RPTED AND LAST_RPTED" column

firmo_sup = firmo_sup.withColumn('dt_first_rpted', trim(firmo_sup.dt_first_rpted)).withColumn('dt_last_rpted', trim(firmo_sup.dt_last_rpted))


# Cleaning date values

firmo_sup = firmo_sup.withColumn('dt_first_rpted', when(col('dt_first_rpted') == '0401-01-01', '1970-01-01').otherwise(firmo_sup.dt_first_rpted))

firmo_sup = firmo_sup.withColumn('dt_last_rpted', when(col('dt_last_rpted') == '0401-01-01', '1970-01-01').otherwise(firmo_sup.dt_last_rpted))

firmo_sup.show()

firmo_sup.count()


# Filtering UNIQUE values

firmo_sup = firmo_sup.dropDuplicates()

firmo_sup.count()


# Cleaning dt_first and last rpted values for "/ into -"

firmo_sup = firmo_sup.withColumn('dt_first_rpted', when(col('dt_first_rpted') == '01/01/0401', '01-01-1800').otherwise(firmo_sup.dt_first_rpted))

firmo_sup = firmo_sup.withColumn('dt_last_rpted', when(col('dt_last_rpted') == '01/01/0401', '01-01-1800').otherwise(firmo_sup.dt_last_rpted))

firmo_sup.show()

# ============================STAGE COMPLETED FOR FIRMO =================




#=======================================ADDR ===========================

# Removing leading and trailing space in "LVID" column

addr = addr.withColumn('lvid', trim(addr.lvid))

addr.show()

# Creating Aliases for easier management  LEFT OUTER JOIN --> FIRMO & ADDR


L = firmo_sup.alias('L')

R = addr.alias('R')


# Joining Tables  LEFT OUTER JOIN FIRMO & ADDR

df_addr = L.join(R, how='left_outer', on='lvid')
                              
df_addr.show() 




# ============================STAGE COMPLETED FOR ADDR =================
