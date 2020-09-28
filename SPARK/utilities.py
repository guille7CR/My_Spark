from pyspark.sql.session import SparkSession
from subprocess import PIPE, Popen
import subprocess
import os

def get_spark_session():
    spark = SparkSession.builder.appName("carpenter") \
        .master("yarn") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "10g") \
        .config("spark.executor.memoryOverhead", "3g") \
        .config("spark.driver.cores", "4") \
        .config("spark.driver.memory", "20g") \
        .config("spark.driver.memoryOverhead", "2g") \
        .config("spark.sql.execution.arrow.enabled", "false") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "10") \
        .config("spark.yarn.queue", "root.bis.std.edf_bis_dd_de") \
        .enableHiveSupport()\
        .getOrCreate()
    
    print("spark-%s.%s" % (os.environ["CDSW_ENGINE_ID"], os.environ["CDSW_DOMAIN"]))
    return spark

def update_table(spark,source, destination, data_schema=None):
    if data_schema == None:
      csv = spark.read.csv(source, header=True, inferSchema=True)
    else:
      csv = spark.read.csv(source, data_schema)
    csv.write.mode("overwrite").saveAsTable(destination)
    spark.sql("REFRESH TABLE {}".format(destination))
    return True

def hdfs_folder_exists(hdfs_folder):
    put = Popen(["hdfs", "dfs", "-ls", hdfs_folder],  stdout=PIPE, stderr=PIPE)
    s_output, s_err = put.communicate()
    if "no such file or directory" in str(s_err).lower():
        return False
    return True

def get_latest_available_folder(root):
  subdirectories = subprocess.check_output(["hdfs","dfs","-ls", root]).split()
  #Clean output and filter by subfoldes with len(yyyymmdd)
  subdirectories = [str(path).replace("'","").split("/")[-1] for path in subdirectories if root in str(path)
                 and len(str(path).replace("'","").split("/")[-1]) == 8]
  subdirectories.sort()
  return subdirectories[-1]