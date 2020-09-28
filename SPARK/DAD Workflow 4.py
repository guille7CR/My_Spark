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
        
      
# Extracting Required Tables
# ==========================

sbu_transfmd = spark.sql('Select Distinct pbin From edf_bis_dd_en.da_sbu_transformed')
da_firmo =  spark.sql('Select * From edf_bis_dd_en.da_firmo')
sic = spark.sql('Select n.supplier_nb, n.lvid, n.dt_first_rpted, n.dt_last_rpted, n.sic_ty_cd as ty_cd, n.sic_cd as value From edf_bis_dd_en.sic_naics_source n left join edf_bis_dd_en.da_sbu_firmo_sup t on n.lvid=t.lvid and n.supplier_nb=t.supplier_nb and n.dt_last_rpted=t.dt_last_rpted and n.sic_cd=t.value where t.dt_last_rpted IS NULL')
supp_info = spark.sql('Select supplier_nb, supplier_nm From edf_bis_dd_en.supplier')


# Creating Aliases For Easier Management
# ======================================

A = sbu_transfmd.alias('A')
B = da_firmo.alias('B')

# Joining FIRMO table
# ==============

df_normalized = A.join(B, how='leftouter',on='pbin')


# Creating Aliases For Easier Management
# ======================================

A = df_normalized.alias('A')
B = sic.alias('B')


# Joining SIC table
# ==================

df_normalized = A.join(B, how='inner',on='lvid')


# Creating Aliases For Easier Management
# ======================================

A = df_normalized.alias('A')
B = supp_info.alias('B')


# Joining SUPPLIER table
# =======================

df_normalized = A.join(B, how='leftouter',on='supplier_nb')


# Eliminating Undesired Columns // Creating Key Column (element) // Renaming child_pbin column
# ======================================================================

columns_to_drop = ['ol_lgl_nm_long','name_norm_long','ain','std_usps_nprf_str','std_city_nm','std_state_cd','std_zip5_cd']

df_normalized = df_normalized.drop(*columns_to_drop).withColumn('element',lit('SIC')).withColumnRenamed("child_pbin", "bin")


# Rearranging Columns
# ================

df_normalized = df_normalized.select("pbin","bin","lvid","value", "element","ty_cd","supplier_nb","dt_first_rpted","dt_last_rpted","supplier_nm")
df_normalized.show()


# Saving Data
# ===========

df_normalized.write.mode("append").insertInto('edf_bis_dd_en.da_sbu_firmo_sup')
