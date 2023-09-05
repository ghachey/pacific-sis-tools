# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
###############################################################################
# This notebook provides some tools for better integration between the        #
# Pacific EMIS and Pacific SIS. In particular useful tools to manage syncing  #
# data of schools.                                                            #
# This notebook can be used for pre-loading the SIS with all the schools      #
# grade levels derived from the enrollments data in the EMIS.                 #
# IMPORTANT: The gradelevels existing data should be cleaned up before        #
# running this notebook (gradelevels-adjustments.sql)                         #
# IMPORTANT: This notebook has a dependent variable from the notebook         #
# sync-staff.ipynb so that one must be run first (at lest first two cells     #
###############################################################################

# Core stuff
import os
import json
import datetime as dt

# Data stuff
import pandas as pd # Data analysis
import numpy as np
import xlrd # excel 
import pyodbc # SQL DB
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import uuid
# Pretty printing stuff
from IPython.display import display, HTML
import pprint
pp = pprint.PrettyPrinter(indent=4)

# Initial setup
cwd = os.getcwd()

# Configuration
with open('config.json', 'r') as file:
     config = json.load(file)
        
# EMIS config
emis_lookup = config['emis_lookup']
emis_school_year = config['emis_school_year']
        
# SIS config
sis_database = config['sis_database']
sis_tenant_id = config['sis_tenant_id']
sis_user_guid = config['sis_user_guid']
sis_country = config['sis_country']
sis_export_data_to_excel = config['sis_export_data_to_excel']
sis_load_data_to_sql = config['sis_load_data_to_sql']

# Config
data_directory = config['data_directory']
country = config['country']
datetime = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# MS SQL Server connection
mssql_connection_string = """
    Driver={{ODBC Driver 17 for SQL Server}};
    Server={},{};
    Database={};
    authentication=SqlPassword;UID={};PWD={};
    TrustServerCertificate=yes;
    autocommit=True
    """.format(config['emis_server_ip'], config['emis_server_port'], config['emis_database'], config['emis_uid'], config['emis_pwd'])

mssql_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": mssql_connection_string})
mssql_engine = create_engine(mssql_connection_url)

# MySQL Connection
mysql_connection_string = "mysql+mysqlconnector://"+config['sis_user']+":"+config['sis_pwd']+"@"+config['sis_host']+":"+config['sis_server_port']+"/"+config['sis_database']
mysql_engine = create_engine(mysql_connection_string)

# %%
# First get the next school_id and school_detail id to be used.
query_gradelevel_next = """
SELECT MAX(`grade_id`) as last_gradelevel_id
FROM `gradelevels`
ORDER BY `grade_id`;
"""

query_schools_gradelevels = """
SELECT DISTINCT SE.[schNo]
	  ,[schName]
      ,[stueClass] GradeCode
	  ,codeDescription Grade
	  ,lvlYear
	  ,ISCED.ilCode ISCEDCode
	  ,ISCED.ilName ISCED
	  ,CASE
		WHEN [stueClass] = 'GPRES' THEN 0
		WHEN [stueClass] = 'GK' THEN 1
		WHEN [stueClass] = 'G1' THEN 2
		WHEN [stueClass] = 'G2' THEN 3
		WHEN [stueClass] = 'G3' THEN 4
		WHEN [stueClass] = 'G4' THEN 5
		WHEN [stueClass] = 'G5' THEN 6
		WHEN [stueClass] = 'G6' THEN 7
		WHEN [stueClass] = 'G7' THEN 8
		WHEN [stueClass] = 'G8' THEN 9
		WHEN [stueClass] = 'G9' THEN 10
		WHEN [stueClass] = 'G10' THEN 11
		WHEN [stueClass] = 'G11' THEN 12
		WHEN [stueClass] = 'G12' THEN 13
		END age_range_id
	  , CASE
		WHEN ISCED.ilCode = 'ISCED 0' THEN 0
		WHEN ISCED.ilCode = 'ISCED 1' THEN 1
		WHEN ISCED.ilCode = 'ISCED 2' THEN 2
		WHEN ISCED.ilCode = 'ISCED 3' THEN 3
		END isced_code
  FROM [dbo].[StudentEnrolment_] SE
  INNER JOIN Schools S ON SE.schNo = S.schNo
  INNER JOIN lkpLevels L ON SE.stueClass = L.codeCode
  INNER JOIN [dbo].[ISCEDLevelSub] ISCEDS ON L.ilsCode = ISCEDS.ilsCode
  INNER JOIN [dbo].[ISCEDLevel] ISCED ON ISCEDS.ilCode = ISCED.ilCode
  WHERE stueYear IN ({},{},{})
  ORDER BY schNo, lvlYear
""".format(emis_school_year-2,emis_school_year-1,emis_school_year)

with mysql_engine.begin() as conn:
    #df_school_calendars = pd.read_sql_query(sa.text(query_school_calendars), conn)
    result1 = conn.execute(sa.text(query_gradelevel_next))
    #result2 = conn.execute(sa.text(query_school_detail_ids))
    last_gradelevel_id = result1.mappings().first()['last_gradelevel_id']
    next_gradelevel_id = last_gradelevel_id+1
    #next_school_detail_id = result2.mappings().first()['last_school_detail_id']+1
    print("Next grade_id should be {}".format(next_gradelevel_id))
    #print("Next school_detail id should be {}".format(next_school_detail_id))
    
with mssql_engine.begin() as conn:
    df_schools_gradelevels = pd.read_sql_query(sa.text(query_schools_gradelevels), conn)
    display(df_schools_gradelevels)
    
# Here we create "template" DataFrames for all the tables of interest.
# They start empty and will later on be populated with data and loaded directly into the SQL DB

query_gradelevels = """
SELECT `grade_id`, `school_id`, `tenant_id`, `age_range_id`, `created_by`, `created_on`, `equivalency_id`, `isced_code`, `next_grade_id`, `short_name`, `sort_order`, `title`, `updated_by`, `updated_on`
FROM `gradelevels`
WHERE tenant_id = '{}' AND grade_id = {};
""".format(sis_tenant_id, next_gradelevel_id)

templates = {
    'gradelevels': {'query': query_gradelevels, 'sql_table': 'gradelevels', 'df_name': None},
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))    

# %%
# Here we first do some hacks to fix the anomolies discovered in the next cell

# Pohnpei seems to have this one student enrollment out of place. So let's just remove the row
df_schools_gradelevels = df_schools_gradelevels.drop(df_schools_gradelevels[(df_schools_gradelevels['schNo'] == 'PNI325') & (df_schools_gradelevels['GradeCode'] == 'G1')].index)

# The Kosrae SMD special schools should be ECE-Grade 8 but has had all sorts of grades over the years. Perhaps easier
# to just populate all of ECE to Grade 12.
# First let's drop all of those gradelevels for that school to start from scratch
df_schools_gradelevels = df_schools_gradelevels.drop(df_schools_gradelevels[(df_schools_gradelevels['schNo'] == 'KSA209')].index)

KSA209 = {
    'schNo':['KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209','KSA209'],
    'schName' :['SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD','SMD'],
    'GradeCode':['GPRES','GK','G1','G2','G3','G4','G5','G6','G7','G8','G9','G10','G11','G12'],
    'Grade':['Pre-school','Grade ECE','Grade 1','Grade 2','Grade 3','Grade 4','Grade 5','Grade 6','Grade 7','Grade 8','Grade 9','Grade 10','Grade 11','Grade 12',],
    'lvlYear':[-1,0,1,2,3,4,5,6,7,8,9,10,11,12],
    'ISCEDCode':['ISCED 0','ISCED 0','ISCED 1','ISCED 1','ISCED 1','ISCED 1','ISCED 1','ISCED 1','ISCED 2','ISCED 2','ISCED 3','ISCED 3','ISCED 3','ISCED 3'],
    'ISCED':['Early Childhood Education','Early Childhood Education','Primary education','Primary education','Primary education','Primary education','Primary education','Primary education',
             'Lower secondary education','Lower secondary education','Upper secondary education','Upper secondary education','Upper secondary education','Upper secondary education'],    
    'age_range_id':[0,1,2,3,4,5,6,7,8,9,10,11,12,13],
    'isced_code':[0,0,1,1,1,1,1,1,2,2,3,3,3,3],
}

df_ksa209 = pd.DataFrame(KSA209)
#print("Manually constructed grades data for school KSA209")
#display(df_ksa209)

df_schools_gradelevels = pd.concat([df_schools_gradelevels, df_ksa209], ignore_index=True)
#display(df_schools_gradelevels)

# %%
# Start processing data into the template DataFrame for SQL load
# WARNING: This cell will print the final DataFrame of gradelevels
# but also raise anomalies that should be manually handled before loading into SIS

df_schools_gradelevels = df_schools_gradelevels.copy()
# Retrieve our list of schools missing from SIS    
# %store -r schools_sis_map

df_schools_gradelevels['grade_id'] = range(next_gradelevel_id, next_gradelevel_id + len(df_schools_gradelevels))
df_schools_gradelevels['school_id'] = df_schools_gradelevels['schName'].map(schools_sis_map)
df_schools_gradelevels['tenant_id'] = sis_tenant_id
df_schools_gradelevels['created_by'] = sis_user_guid
df_schools_gradelevels['created_on'] = datetime
df_schools_gradelevels['updated_by'] = np.NaN
df_schools_gradelevels['updated_on'] = np.NaN
df_schools_gradelevels = df_schools_gradelevels.rename(columns={'lvlYear': 'equivalency_id', 'GradeCode': 'short_name', 'Grade': 'title'})
df_schools_gradelevels['sort_order'] = df_schools_gradelevels['equivalency_id']+2
df_schools_gradelevels['next_grade_id'] = np.NaN

# Derive next grade levels

for i in range(0, len(df_schools_gradelevels)-1):
    if df_schools_gradelevels.loc[i+1, 'school_id'] == df_schools_gradelevels.loc[i, 'school_id']:
        if df_schools_gradelevels.loc[i+1, 'equivalency_id'] != df_schools_gradelevels.loc[i, 'equivalency_id']+1:
            sis_load_data_to_sql = False # Avoid loading if we get any anomalies
            print("Anomaly with school {} and grade {} not following grade {}".format(df_schools_gradelevels.loc[i, 'school_id'], df_schools_gradelevels.loc[i+1, 'short_name'], df_schools_gradelevels.loc[i, 'short_name']))
        df_schools_gradelevels.loc[i, 'next_grade_id'] = df_schools_gradelevels.loc[i+1, 'grade_id']
    else:
        df_schools_gradelevels.loc[i, 'next_grade_id'] = np.NaN
        
    # Print out anomalies. When a school has grades that or not subsequent of another
    
        
# Final sort to avoid SQL violation of foreign keys by loading next grades before
# It seems this is not necessary in one single transaction
#df_schools_gradelevels.sort_values(by=['grade_id'], ascending=False, inplace=True)

df_schools_gradelevels = df_schools_gradelevels[['grade_id','school_id','tenant_id','age_range_id','created_by','created_on','equivalency_id','isced_code','next_grade_id','short_name','sort_order','title','updated_by','updated_on']]

print("Final Grade Levels DataFrame")
display(df_schools_gradelevels)

# %%
# %%time

if sis_export_data_to_excel == True:
    
    # Write to Excel the sample template data
    with pd.ExcelWriter(data_directory + '/' + country + '/sis-schools-grade-levels-template-sample-data.xlsx') as writer:

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
            
     # Write to Excel the data for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-schools-grade-levels-to-insert-data.xlsx') as writer:

        print("Saving gradelevels with all final data records to Excel")
        df_schools_gradelevels.to_excel(writer, index=False, sheet_name='user_master')
else:
    print("Not exporting data to excel")

# %%
# %%time

# Load all data into the database
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:        
        print("Loading gradelevels with all final data")
        df_schools_gradelevels.to_sql('gradelevels', con=conn, index=False, if_exists='append')        
        
    print("All gradelevels imported successfully")
else:
    print("Not loading the data into SQL")
