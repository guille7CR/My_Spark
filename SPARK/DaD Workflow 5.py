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

 
sbu_trans = spark.sql('Select Distinct pbin From edf_bis_dd_en.da_sbu_transformed')

da_firmo= spark.sql('Select * From edf_bis_dd_en.da_firmo')

sbu_trans.show()

da_firmo.show()

#=======================================

#Count total of records before transformation

#sbu_trans.count()
#da_trans.count()

#=======================================

# Creating Aliases for easier management

L = sbu_trans.alias('L')

R = da_firmo.alias('R')


# Joining Tables  LEFT OUTER JOIN

df_normalized = L.join(R, how='leftouter', on='pbin')

#=======================================

# Dropping extra columns and renaing child pbin

columns_to_drop = ['ol_lgl_nm_long','name_norm_long','ain','std_usps_nprf_str','std_city_nm','std_state_cd','std_zip5_cd']

df_normalized = df_normalized.drop(*columns_to_drop)

df_normalized = df_normalized.withColumnRenamed("child_pbin", "bin")
                                
df_normalized.show()                           

#=======================================

# Extracting require tables  BUS_PHONE_SOURCE

bus_phone= spark.sql('Select n.lvid, n.supplier_nb, n.dt_first_rpted, n.dt_last_rpted,n.ph_area_cd, t.ty_cd From edf_bis_dd_en.bus_phone_source n left join edf_bis_dd_en.da_sbu_firmo_sup t on n.lvid=t.lvid and n.supplier_nb=t.supplier_nb and n.dt_last_rpted=t.dt_last_rpted and n.ph_area_cd=t.value where t.dt_last_rpted IS NULL')

bus_phone.show()

#=======================================

# Creating Aliases for easier management

L = df_normalized.alias('L')

R = bus_phone.alias('R')


# Joining Tables INNER JOIN

df_normalized = L.join(R, how='inner', on='lvid')

df_normalized.show()

#=======================================

# Creating key value pair (element-value)

df_normalized = df_normalized.withColumn('element',lit('Phone_AREA'))\
.withColumn("value", df_normalized["ph_area_cd"])
                                
df_normalized.show()

#=======================================

# Extracting require tables  SUPPLIER

supp_nm= spark.sql('Select supplier_nb, supplier_nm  From edf_bis_dd_en.supplier')

supp_nm.show()

#=======================================


# Creating Aliases for easier management

L = df_normalized.alias('L')

R = supp_nm.alias('R')


# Joining Tables INNER JOIN

df_normalized = L.join(R, how='leftouter', on='supplier_nb')

columns_to_drop = ['ph_area_cd']

df_normalized = df_normalized.drop(*columns_to_drop)

df_normalized.show()

df_normalized.dtypes

#=======================================

#Rearrange columns

df_normalized= df_normalized.select("pbin","bin","lvid","value", "element","ty_cd","supplier_nb","dt_first_rpted","dt_last_rpted","supplier_nm")

df_normalized.show()

#=======================================

# Saving Data   

df_normalized.write.mode("append").insertInto('edf_bis_dd_en.da_sbu_firmo_sup')

spark.sql('SELECT * from edf_bis_dd_en.da_sbu_firmo_sup').show()