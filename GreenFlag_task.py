# Please install the required modules in the system to execute the code successfully
# import section
import pandas as pd
import glob
import pandasql as ps

# Variable declaration
path = '/home/pankaj/Desktop/Job_description/Direct_line_group_task/Data Engineer Test _ Green Flag' # use your path
file_list = []
q1="""SELECT DATETIME(ObservationDate) AS "Hottest_Date",ScreenTemperature AS "Hottest_Temperature",Region AS "Hottest_Region" FROM dframe_required where ScreenTemperature = (SELECT max(ScreenTemperature) FROM dframe_required)"""

# Function declaration 
# function 'list_filename_fn' reads CSV file from the given directory path and process that data into a dataframe 
#and assign it to a list variable and finally returns it.
def list_filename_fn(file_path):
	all_files = glob.glob(file_path + "/*.csv")
	for filename in all_files:
		weather_df1 = pd.read_csv(filename, index_col=None, header=0)
		file_list.append(weather_df1)
	return file_list

# function 'df_function' combines the data of all the files in a single dataframe and convert it into 'parquet' format.
# execute the passed SQL query to provide the required output data.
def df_function(file_list_var,query_var):
	dframe = pd.concat(file_list, axis=0, ignore_index=True)
	dframe.drop_duplicates()
	dframe_required = dframe[['ObservationDate','ScreenTemperature','Region']]
	dframe_required.to_parquet('weather_parquet.parquet',row_group_size=1048,engine="pyarrow")
	return ps.sqldf(query_var)

# function call by passing the required values and printing the output
print('The required information from the data is: \n',df_function(list_filename_fn(path),q1))

# function 'test_check_nan' checks if the required column contains any NaN values in it.
def test_check_nan():
	assert df_function(list_filename_fn(path),q1) == dframe_required['ObservationDate'].isna().sum()<1
	assert df_function(list_filename_fn(path),q1) == dframe_required['ScreenTemperature'].isna().sum()<1
	assert df_function(list_filename_fn(path),q1) == dframe_required['Region'].isna().sum()<1


