from subprocess import PIPE , Popen
import subprocess
import os
import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession

 
# Setting Spark session
# ====================================

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
		
		
# CONTAINER   CREATE GOVERMENT FLAG
# ==================================== 

# Truncate govt flag table
# ====================================
                  
query = """TRUNCATE TABLE edf_bis_dd_en.da_govt_flag"""
                  
spark.sql(query)
                  
govt_tbl = spark.sql('''

SELECT * FROM edf_bis_dd_en.da_govt_flag

''')

govt_tbl.show()

        
# Extracting Required Tables
# ==========================

gover = spark.sql('''

Select sns.lvid, 
sns.naics_std_cd, 
blv.ain, 
concat
(
case when am.std_hse_nb is null then '' else am.std_hse_nb end, 
'|',
case when am.std_pre_direction is null then '' else am.std_pre_direction end,
'|',
case when am.std_str_addr is null then '' else am.std_str_addr end,
'|',
case when am.std_pst_direction is null then '' else am.std_pst_direction end,
'|',
case when am.std_unit_nb is null then '' else am.std_unit_nb end,
'|',
case when am.std_zip5_cd is null then '' else am.std_zip5_cd end
) as no_dir_strtnm_ste_zip
From edf_bis_dd_en.address am 
Inner Join edf_bis_dd_en.bus_loc_var blv On blv.ain = am.ain 
Inner Join edf_bis_dd_en.sic_naics_source sns On sns.lvid = blv.lvid
Inner Join edf_bis_dd_en.pbin_lvid_assoc pla on pla.lvid = blv.lvid
Inner Join edf_bis_dd_en.bus_oper_loc bol on pla.pbin = bol.pbin
where
(substr(sns.naics_std_cd,1,2) = '92' and 
supplier_nb != 508673) and

(substr(sns.dt_last_rpted,7,4) = '2020' or 
substr(sns.dt_last_rpted,7,4) = '2019' or
substr(sns.dt_last_rpted,7,4) = '2018' or 
substr(sns.dt_last_rpted,7,4) = '2017') and

(substr(bol.ol_activity_ind, 1, 1) = 'A' 
Or substr(bol.ol_activity_ind, 1, 1) = 'B' 
Or substr(bol.ol_activity_ind, 1, 1) = 'C') 

''')
                  
# Grouping data and adding govt column
# ====================================
                  
flag_table = gover.groupBy("no_dir_strtnm_ste_zip").count().withColumn('govt',lit('1')).drop('count') 
                  
                  
                  
                  
# Saving Data
# ===========

flag_table.write.mode("append").insertInto('edf_bis_dd_en.da_govt_flag')		
		
 
# CONTAINER 2  ADDRESS MOD BUILD
# ==================================== 
        
# Truncate ADDRESS MOD TEMP TBL
# ====================================
                  
query = """TRUNCATE TABLE edf_bis_dd_en.da_address_mod_temp"""
                  
spark.sql(query)
                  
temp_tbl = spark.sql('''

SELECT * FROM edf_bis_dd_en.da_address_mod_temp

''')

temp_tbl.show()       
        
        

        
# Extracting require tables
# ====================================

govt_flg= spark.sql('''

Select gf.no_dir_strtnm_ste_zip,
gf.govt as govt_flag
From 
edf_bis_dd_en.da_govt_flag gf

''')

govt_flg.show()

govt_flg.count()



address_mod = spark.sql('''

Select *,

CASE 
WHEN (std_zip_cd_stat_cd='A' 
or std_zip_cd_stat_cd='C')
AND std_rtn_cd is null
AND (std_usps_rc_ty_cd='F' 
or std_usps_rc_ty_cd='G' 
or std_usps_rc_ty_cd='H' 
or std_usps_rc_ty_cd='P'
or std_usps_rc_ty_cd='R' 
or std_usps_rc_ty_cd='S')
THEN '1' ELSE '0'
END as std_addr,

CASE 
WHEN (std_zip_cd_stat_cd='A' 
or std_zip_cd_stat_cd='C')
AND std_rtn_cd is null
AND (std_usps_rc_ty_cd='F' 
or std_usps_rc_ty_cd='H' 
or std_usps_rc_ty_cd='S')
THEN '1' ELSE '0'
END as verified_addr,

case 
when std_usps_rc_ty_cd = 'H'
and std_unit_nb is null then '1'
when std_usps_rc_ty_cd = 'H'
and std_unit_nb is not null then '0'
end as miss_ste_addr,

case when std_usps_rc_ty_cd = 'H'
and std_unit_nb is not null
and std_ovall_prbl_cor != '0'
then '1'
when `std_usps_rc_ty_cd` = 'H'
and (std_unit_nb is not null or 
std_unit_nb is null)
and std_ovall_prbl_cor = '0'
then '0'
end as innac_ste_addr,

case 
when std_usps_rc_ty_cd = 'S'
and std_unit_nb is not null
and std_ovall_prbl_cor != '0'
then '1'
when std_usps_rc_ty_cd = 'S'
and std_unit_nb is null
and std_ovall_prbl_cor = '0'
then '0'
end as ste_for_single_loc_addr,

case 
when hyg_cmra_in = 'Y' 
then '1'
else '0'
end as cmra,

case 
when std_usps_rc_ty_cd = 'H'
then '1'
else '0'
end as multiunit_addr,

case when std_usps_rc_ty_cd = 'S'
then '1'
else '0'
end as single_loc_addr, 

concat
(
case when std_hse_nb is null then '' else std_hse_nb end, 
'|',
case when std_pre_direction is null then '' else std_pre_direction end,
'|',
case when std_str_addr is null then '' else std_str_addr end,
'|',
case when std_pst_direction is null then '' else std_pst_direction end,
'|',
case when std_zip5_cd is null then '' else std_zip5_cd end
) as no_dir_strtnm_zip,

concat
(
case when std_hse_nb is null then '' else std_hse_nb end, 
'|',
case when std_pre_direction is null then '' else std_pre_direction end,
'|',
case when std_str_addr is null then '' else std_str_addr end,
'|',
case when std_pst_direction is null then '' else std_pst_direction end,
'|',
case when std_unit_nb is null then '' else std_unit_nb end,
'|',
case when std_zip5_cd is null then '' else std_zip5_cd end
) as no_dir_strtnm_ste_zip


From edf_bis_dd_en.address 

''')

address_mod.show()
  
address_mod.count()
  

# Trimming columns to remove any space
# ====================================
  
govt_flg = govt_flg.withColumn('govt_flag', trim(govt_flg.govt_flag)).withColumn('no_dir_strtnm_ste_zip', trim(govt_flg.no_dir_strtnm_ste_zip))

address_mod = address_mod.withColumn('verified_addr', trim(address_mod.verified_addr)).withColumn('hyg_cmra_in', trim(address_mod.hyg_cmra_in)).withColumn('hyg_residential_in', trim(address_mod.hyg_residential_in)).withColumn('std_addr', trim(address_mod.std_addr)).withColumn('miss_ste_addr', trim(address_mod.miss_ste_addr)).withColumn('innac_ste_addr', trim(address_mod.innac_ste_addr)).withColumn('no_dir_strtnm_ste_zip', trim(address_mod.no_dir_strtnm_ste_zip))

 

#Adding new columns and Setting flags
  

address_mod = address_mod.withColumn("rsdl", when((col("verified_addr") == "1") & ((col("hyg_cmra_in") == "N") | (col("hyg_cmra_in").isNull())) & (col("hyg_residential_in") == "Y"), "1").otherwise("0"))
                                     
address_mod = address_mod.withColumn("comml", when((col("verified_addr") == "1") & ((col("hyg_residential_in") == "N") | (col("hyg_residential_in").isNull())), "1").otherwise("0"))
  
address_mod = address_mod.withColumn("non_phys", when((col("std_addr") == "1") & (col("verified_addr") == "0"), "1").otherwise("0"))

address_mod = address_mod.withColumn("comp_addr", when((col("verified_addr") == "1") & ( (col("miss_ste_addr") == "0") | (col("miss_ste_addr").isNull())) & ( (col("innac_ste_addr") == "0") | (col("innac_ste_addr").isNull())) & ( (col("ste_for_single_loc_addr") == "0") | (col("ste_for_single_loc_addr").isNull())), "1").otherwise(when((col("verified_addr") == "1") & ((col("miss_ste_addr") == "1") | (col("innac_ste_addr") == "1") | (col("ste_for_single_loc_addr") == "1")),("0")))) 

    
 
address_mod.count()
  



# Testing counts as per 1s and 0s
# ====================================
  
  
rsdl_one= address_mod.filter(address_mod.rsdl == "1")
  
rsdl_zero= address_mod.filter(address_mod.rsdl == "0")

# RSDL
# ====================================
  
rsdl_one.count()

rsdl_zero.count()
  

# COMML
# ====================================
comml_one= address_mod.filter(address_mod.comml == "1")
  
comml_zero= address_mod.filter(address_mod.comml == "0")
  
comml_one.count()

comml_zero.count()
  
  
  
# COMP_ADDR
# ====================================  
addr_one= address_mod.filter(address_mod.comp_addr == "1")
  
addr_zero= address_mod.filter(address_mod.comp_addr == "0")
  
addr_one.count()

addr_zero.count()
  

# NON_PHYS
# ====================================
non_phys_one= address_mod.filter(address_mod.non_phys == "1")
  
non_phys_zero= address_mod.filter(address_mod.non_phys == "0")
  
non_phys_one.count()

non_phys_zero.count()


  
  
# Creating Aliases for easier management  ADDRESS_MOD VS GOVT_FLAG
# ====================================

L = address_mod.alias('L')

R = govt_flg.alias('R')


#Joining Tables LEFT OUTER JOIN ADDRESS_MOD VS GOVT_FLAG

address_mod = L.join(R, how='left_outer', on='no_dir_strtnm_ste_zip')

address_mod.show()
  

# Adding govt column and Setting flag
# ====================================
  
  
address_mod = address_mod.withColumn("govt", when((col("verified_addr") == "1") & (col("govt_flag") == "1") & (col("comml") == "1"), "1").otherwise("0"))
  



# Rearranging dataframe before writing 
# ====================================

address_mod= address_mod.select("ain","std_addr_hsh_nb","std_hse_nb","std_str_addr","std_str_sfx","std_pre_direction","std_pst_direction","std_zip4_cd","std_zip5_cd","std_unit_ty_cd","std_unit_nb","std_unit_rc","std_po_box_nb","std_carr_rout","std_state_cd","std_city_nm","std_sh_city_nm","std_carr_rout_rc","std_city_state_rc","std_fips_state_cd","std_fips_cnty_cd","std_usps_cnty_nm","std_msa_cd","std_long_lat_level","std_latitude","std_latitude_dir","std_longitude","std_longitude_dir","std_rtn_cd","std_usps_rc_ty_cd","std_zip5_rc","std_zip4_rc","std_sa_rc","std_g9_rc","std_zip_cd_stat_cd","std_census_tract","std_rur_hw_cntr_ty","std_rur_hw_cntr_nb","std_prv_mailbox_nb","std_firm_rc","std_ovall_prbl_cor","std_str_nm_sc","std_addr_prbl_cor","std_delv_seq_fnote","std_delv_pt_bar_cd","std_delv_pt_bar_ad","std_delv_cd","std_upd_dt","std_db_ver_nb","std_usps_nprf_city","std_usps_nprf_str","std_usps_nprf_rc","std_usps_nprf_rcty","hyg_dsf2_bus_in","hyg_dsf_cd","hyg_dsf_dwg_cd","hyg_dsf_delv_ty","hyg_dsf_aqi_cd","hyg_dsf_seas_cd","hyg_dsf_vacant_in","hyg_residential_in","hyg_dwg_cd","hyg_dsf_fnote","hyg_zap_in","hyg_proc_dt","hyg_dsf_drop_in","hyg_dsf2_vrf_cd","hyg_dsf2_rec_ty_cd","hyg_software_cd","hyg_cmra_in","hyg_delv_pt_bar_cd","hyg_lacs_in","hyg_lacs_dt","create_db_dt","upd_db_dt","upd_db_id","std_addr","verified_addr","rsdl","comml","non_phys","comp_addr","miss_ste_addr","innac_ste_addr","ste_for_single_loc_addr","cmra","multiunit_addr","single_loc_addr","no_dir_strtnm_zip","no_dir_strtnm_ste_zip","govt")

address_mod.show()

address_mod.count()
  
  
# Writing Data
# ====================================

address_mod.write.mode("append").insertInto('edf_bis_dd_en.da_address_mod_temp')

spark.sql('SELECT * from edf_bis_dd_en.da_address_mod_temp').show()