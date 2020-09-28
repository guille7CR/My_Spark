import os
from datetime import datetime, timedelta
import json
import utilities as utl
from pytz import timezone


if __name__ == "__main__":
    spark = utl.get_spark_session()
    updated, no_update_needed, fail = [],[],[]

    with open("table_info.json") as json_file:
        data = json.load(json_file)
        for table_info in data:
            print("Working on table: {}".format(table_info))
            source = data[table_info]["source"]

            # Get last update date from config file
            last_updated_str = data[table_info]["last_update"]
            last_updated_dt = datetime.strptime(last_updated_str, "%Y%m%d")
          
            update_freq = data[table_info]["update_freq"]
            next_update = last_updated_dt + timedelta(days=update_freq)
            today_date = datetime.now(timezone("US/Central")).date()
            
            # Check if an update is needed
            if update_freq == 0 or next_update.date() >= today_date :
              no_update_needed.append(table_info)              
            elif next_update.date() <= today_date :

                          
              # Get last available folder from hdfs 
              most_recent_available_folder = utl.get_latest_available_folder(source)
              mraf_dt = datetime.strptime(most_recent_available_folder, "%Y%m%d")

              # Check if we have the latest of the dataset version already
              if last_updated_str != most_recent_available_folder:
                destination = data[table_info]["destination"]
                # Update based on latest available source
                if utl.update_table(spark, os.path.join(source,most_recent_available_folder), destination):
                  updated.append(table_info)
                  data[table_info]["last_update"] = most_recent_available_folder
                  # Check if we have the proper dataset based on the update frequency
                  if next_update > mraf_dt:
                    print( "WARNING: The data source used {}: is older than the expected {}".format(most_recent_available_folder,next_update.strftime("%Y%m%d")))

                else:
                  print("Failed to update based on source folder:{}".format(source))
                  fail.append((table_info,"Failed to update based on source folder: {}".format(source)))
              else:
                print("Failed:".format(source))
                fail.append((table_info,"Failed,not a newer data source was found: {}.Shall we reach OneSearch for help?".format(source)))

            else:
                no_update_needed.append(table_info)
    
    # Update source json
    with open("table_info.json", "w") as f:
        json.dump(data, f, indent=4, sort_keys=True)

    print("Successfully updated:"+str(updated))
    print("No update needed:"+str(no_update_needed))
    print("Failed to update:"+str(fail))
    spark.stop()