import pandas as pd
import subprocess
from multiprocessing import Pool
import glob
import time
import os

start_time = time.time()

#load data into pandas dataframe
# wrap your csv importer in a function that can be mapped
def read_file(filename):
  print(filename)
  if os.path.getsize(filename)>0:
    temp = pd.read_csv(filename,sep=",",header=None,na_values=['\\N'])
    temp.columns = table_columns
    return temp
  else:
    return None
	
#put final dataframe
df_list = None

#load data
file_list = glob.glob(local_path+'/'+table_name+'/*')
with Pool(processes=no_of_CPUs*2) as pool:
  df_list = pool.map(read_file, file_list)

# reduce the list of dataframes to a single dataframe
combined_df = pd.concat(df_list, ignore_index=True)

print("--- %s seconds ---" % (time.time() - start_time))

#free up space
df_list = None

#execute if you want to clean up directory
#rmtree(local_path+'/'+table_name+"/")



