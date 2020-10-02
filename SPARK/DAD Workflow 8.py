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
sls_sz1 =  spark.sql('Select n.lvid, n.supplier_nb, n.dt_last_rpted, n.dt_first_rpted, n.act_sls_sz as a_act_sls_sz, n.emp_sls_sz_ty_cd as a_emp_sls_sz_ty_cd , n.act_emp_sz as a_act_emp_sz , n.sls_sz_cd as a_sls_sz_cd From edf_bis_dd_en.emp_sales_size_source n left join edf_bis_dd_en.da_sbu_firmo_sup t on n.lvid=t.lvid and n.supplier_nb=t.supplier_nb and n.dt_last_rpted=t.dt_last_rpted and n.act_emp_sz=t.value where t.dt_last_rpted IS NULL')
sls_sz2 =  spark.sql('Select n.lvid, n.supplier_nb, n.dt_last_rpted, n.dt_first_rpted, n.act_sls_sz as b_act_sls_sz, n.emp_sls_sz_ty_cd as b_emp_sls_sz_ty_cd , n.act_emp_sz as b_act_emp_sz , n.sls_sz_cd as b_sls_sz_cd From edf_bis_dd_en.emp_sales_size_source n left join edf_bis_dd_en.da_sbu_firmo_sup t on n.lvid=t.lvid and n.supplier_nb=t.supplier_nb and n.dt_last_rpted=t.dt_last_rpted and n.act_sls_sz=t.value where t.dt_last_rpted IS NULL')
supp_info = spark.sql('Select supplier_nb, supplier_nm From edf_bis_dd_en.supplier')


# Creating Aliases For Easier Management
# ======================================

A = sbu_transfmd.alias('A')
B = da_firmo.alias('B')

# Joining tables (Sbu, Firmo)
# ==============

df_normalized = A.join(B, how='leftouter',on='pbin')


# Eliminating Undesired Columns & Renaming child_pbin column
# ======================================================================

columns_to_drop = ['ol_lgl_nm_long','name_norm_long','ain','std_usps_nprf_str','std_city_nm','std_state_cd','std_zip5_cd']

df_normalized = df_normalized.drop(*columns_to_drop).withColumnRenamed("child_pbin", "bin")


# Creating Aliases For Easier Management
# ======================================

A = sls_sz1.alias('A')
B = sls_sz2.alias('B')

# Joining sls tables
# ==============

sls_normalized = A.join(B, how='inner',on=['lvid','supplier_nb','dt_last_rpted','dt_first_rpted'])


# Creating Aliases For Easier Management
# ======================================

A = df_normalized.alias('A')
B = sls_normalized.alias('B')

# Joining tables (Normalized and sls normalized)
# ==============

df_normalized = A.join(B, how='inner',on=['lvid'])


# Creating Sls_Size dataset
# =========================

df_normalized1 = df_normalized.withColumn('element',lit('Sls_Size')).withColumn('b_act_sls_sz',regexp_replace('b_act_sls_sz','[^0-9].*',''))

columns_to_drop1 = ['a_emp_sls_sz_ty_cd', 'a_act_emp_sz','a_act_sls_sz','b_emp_sls_sz_ty_cd','b_act_emp_sz','a_sls_sz_cd']

df_normalized1 = df_normalized1.drop(*columns_to_drop1).withColumnRenamed("b_act_sls_sz", "value").withColumnRenamed("b_sls_sz_cd", "ty_cd")


# Creating Emp_Size dataset
# =========================

df_normalized2 = df_normalized.withColumn('element',lit('Emp_Size'))

columns_to_drop2 = ['a_act_sls_sz', 'a_sls_sz_cd', 'b_act_sls_sz', 'b_emp_sls_sz_ty_cd', 'b_act_emp_sz', 'b_sls_sz_cd']

df_normalized2 = df_normalized2.drop(*columns_to_drop2).withColumnRenamed("a_act_emp_sz", "value").withColumnRenamed("a_emp_sls_sz_ty_cd", "ty_cd")


# Union of Sls and Emp size datasets
# ======================================

df_normalized = df_normalized1.union(df_normalized2)


# Creating Aliases For Easier Management
# ======================================

A = df_normalized.alias('A')
B = supp_info.alias('B')


# Joining SUPPLIER table
# =======================

df_normalized = A.join(B, how='leftouter',on='supplier_nb')


# Rearranging Columns
# ================

df_normalized = df_normalized.select("pbin","bin","lvid","value", "element","ty_cd","supplier_nb","dt_first_rpted","dt_last_rpted","supplier_nm")
df_normalized.show()

# Saving Data
# ===========

df_normalized.write.mode("append").insertInto('edf_bis_dd_en.da_sbu_firmo_sup')

