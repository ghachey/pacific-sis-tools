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
# Pacific EMIS and Pacific SIS. In particular useful tools to manage          #
# producing a default configuration for attendance. This is not based on the  #
# EMIS as such granular attendance feature is only found in the SIS           #
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
data_directory = config['data_directory']
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

print("Retrieving settings and creating database connections")

# %%
# Get some auxilairy data (mappings, next IDs, templates, etc.)

# Get the schools with no configured attendance
query_schools_with_no_attendace_sis = """
SELECT school_id FROM school_master WHERE 
school_id NOT IN (SELECT school_id FROM attendance_code_categories WHERE tenant_id = '{}')
AND
tenant_id = '{}';
""".format(sis_tenant_id, sis_tenant_id)

with mysql_engine.begin() as conn:
    df_schools_with_no_attendace_sis = pd.read_sql_query(sa.text(query_schools_with_no_attendace_sis), conn)
    print("Schools with no attendance configured")
    display(df_schools_with_no_attendace_sis)    
    
# Here we create "template" DataFrames for all the tables of interest.
# They start empty and will later on be populated with data and loaded directly into the SQL DB

query_attendance_code_categories = """
SELECT `attendance_category_id`, `school_id`, `tenant_id`, `academic_year`, `created_by`, `created_on`, `rollover_id`, `title`, `updated_by`, `updated_on`
FROM `attendance_code_categories`
WHERE tenant_id = '{}' AND school_id = {} AND attendance_category_id = {};
""".format(sis_tenant_id, 115, 1)

query_attendance_code = """
SELECT `attendance_category_id`, `attendance_code`, `school_id`, `tenant_id`, `academic_year`, `allow_entry_by`, `created_by`, `created_on`, `default_code`, `rollover_id`, `short_name`, `sort_order`, `state_code`, `title`, `type`, `updated_by`, `updated_on`
FROM `attendance_code`
WHERE tenant_id = '{}' AND school_id = {} AND attendance_category_id = {};
""".format(sis_tenant_id, 115, 1)

templates = {
    'attendance_code_categories': {'query': query_attendance_code_categories, 'sql_table': 'attendance_code_categories', 'df_name': None},
    'attendance_code': {'query': query_attendance_code, 'sql_table': 'attendance_code', 'df_name': None},
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))

# %%
school_ids = df_schools_with_no_attendace_sis['school_id'].values
schools_num = len(school_ids)
print("Number of schools without attendance configuration: {}".format(schools_num))

if schools_num > 0:
    ###############################################################################
    # Prepare the attendance_code_categories DataFrame for all the schools using the template DataFrame
    ###############################################################################
    df_attendance_code_categories_all = pd.concat([templates['attendance_code_categories']['df']]*schools_num, ignore_index=True)
    df_attendance_code_categories_all['school_id'] = df_schools_with_no_attendace_sis['school_id']
    df_attendance_code_categories_all['created_by'] = sis_user_guid
    df_attendance_code_categories_all['created_on'] = datetime
    display(df_attendance_code_categories_all)
    
    ###############################################################################
    # Prepare the attendance_code DataFrame for all the schools using the template DataFrame
    ###############################################################################
    df_attendance_code_all = pd.concat([templates['attendance_code']['df']]*schools_num, ignore_index=True)
    df_school_ids = pd.DataFrame(data=df_schools_with_no_attendace_sis['school_id'].repeat(5))
    df_school_ids.reset_index(drop=True, inplace=True)
    df_attendance_code_all['school_id'] = df_school_ids['school_id']
    df_attendance_code_all['created_by'] = sis_user_guid
    df_attendance_code_all['created_on'] = datetime
    display(df_attendance_code_all)

# %%
# %%time

if sis_export_data_to_excel == True:
    
    # Write to Excel the sample template data
    with pd.ExcelWriter(data_directory + '/' + country + '/sis-attendance-template-sample-data.xlsx') as writer:        

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
            
     # Write to Excel the data for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-attendance-to-insert-data.xlsx') as writer:        

        print("Saving attendance_code_categories with all final data records to Excel")
        df_attendance_code_categories_all.to_excel(writer, index=False, sheet_name='attendance_code_categories')
        print("Saving attendance_code with all final data records to Excel")
        df_attendance_code_all.to_excel(writer, index=False, sheet_name='attendance_code')
else:
    print("Not exporting data to excel")

# %%
# %%time

# Load all data into the database
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:
        print("Loading attendance_code_categories with all final data")
        df_attendance_code_categories_all.to_sql('attendance_code_categories', con=conn, index=False, if_exists='append')
        print("Loading attendance_code with all final data")
        df_attendance_code_all.to_sql('attendance_code', con=conn, index=False, if_exists='append')
        
    print("All attendance configuration imported successfully")
else:
    print("Not loading the data into SQL")
