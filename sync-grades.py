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
# This notebook is a simpley utility to pre-load default grade scales for all #
# schools in the SIS with no such existing configuration                      #
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

print("Retrieving settings and creating database connections")

# %%
# Get some auxilairy data (mappings, next IDs, templates, etc.)

# Get the schools with no configured subject
# Hard coded stuff to get this working in a quick and dirty way
query_schools_with_no_subject_sis = """
SELECT school_id FROM school_master WHERE 
school_id NOT IN (26,27,28,40,41,42,43,44,45,46,47)
AND
tenant_id = '{}';
""".format(sis_tenant_id, sis_tenant_id)

# All school_ids in SIS with previous year subjects
query_schools_with_subject_sis = """
SELECT school_id FROM school_master WHERE tenant_id = '{}' AND school_id IN (26,27,28,40,41,42,43,44,45,46,47);
""".format(sis_tenant_id)

with mysql_engine.begin() as conn:
    df_schools_with_no_subject_sis = pd.read_sql_query(sa.text(query_schools_with_no_subject_sis), conn)
    print("Schools with no subject configured")
    display(df_schools_with_no_subject_sis)
    df_schools_with_subject_sis = pd.read_sql_query(sa.text(query_schools_with_subject_sis), conn)
    print("Schools with subject configured previous year")
    display(df_schools_with_subject_sis)    
    
# Here we create "template" DataFrames for all the tables of interest.
# They start empty and will later on be populated with data and loaded directly into the SQL DB
# There is some hard coded shit here (I needed something extremely quick and dirty)
# template for all remaining schools
query_subjects = """
SELECT `tenant_id`, `school_id`, `subject_id`, `subject_name`, `created_by`, `created_on`, `updated_by`, `updated_on`, `academic_year`, `rollover_id`
FROM `subject`
WHERE `academic_year` = 2021
""".format(sis_tenant_id)

# template for schools with records also in previous year
query_subjects2 = """
SELECT `tenant_id`, `school_id`, `subject_id`, `subject_name`, `created_by`, `created_on`, `updated_by`, `updated_on`, `academic_year`, `rollover_id`
FROM `subject`
""".format(sis_tenant_id)

templates = {
    'subject': {'query': query_subjects, 'sql_table': 'subject', 'df_name': None},
    'subject2': {'query': query_subjects2, 'sql_table': 'subject', 'df_name': None},
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))

# %%
# Schools that had subjects in previous year. So create two sets
# 26,27,28,40,41,42,43,44,45,46,47

template_school_id = templates['subject2']['df']['school_id'].unique()[0]
school_ids_no_subject = df_schools_with_no_subject_sis['school_id'].values
school_ids_with_subject = [26,27,28,40,41,42,43,44,45,46,47] #df_schools_sis['school_id'].values
schools_num_no_subject = len(school_ids_no_subject)
schools_num_with_subject = len(school_ids_with_subject)
print("Number of schools without subject configuration: {}".format(schools_num_no_subject))
print("Number of schools with subject in previous year configuration: {}".format(schools_num_with_subject))

    
# Prepare a dataset for all schools with no subject
if schools_num_no_subject > 0:
    ###############################################################################
    # Prepare the subject DataFrame for all the schools using the template DataFrame
    ###############################################################################
    df_subjects_all = pd.concat([templates['subject']['df']]*schools_num_no_subject, ignore_index=True)
    df_subjects_all['tenant_id'] = sis_tenant_id
    df_school_ids = pd.DataFrame(data=df_schools_with_no_subject_sis['school_id'].repeat(6))
    df_school_ids.reset_index(drop=True, inplace=True)
    df_subjects_all['school_id'] = df_school_ids['school_id']    
    df_subjects_all['created_by'] = sis_user_guid
    df_subjects_all['created_on'] = datetime
    df_subjects_all['academic_year'] = 2022
    display(df_subjects_all)
    
# Prepare a dataset for schools already in the system (with previous year subjects)
if schools_num_with_subject > 0:
    ###############################################################################
    # Prepare the subject DataFrame for all the schools using the template DataFrame
    ###############################################################################
    df_subjects2_all = pd.concat([templates['subject2']['df']]*schools_num_with_subject, ignore_index=True)
    df_subjects2_all['tenant_id'] = sis_tenant_id
    df_school_ids = pd.DataFrame(data=df_schools_with_subject_sis['school_id'].repeat(12))
    df_school_ids.reset_index(drop=True, inplace=True)
    df_subjects2_all['school_id'] = df_school_ids['school_id']    
    df_subjects2_all['created_by'] = sis_user_guid
    df_subjects2_all['created_on'] = datetime        
    #df_subjects2_all.loc[df_subjects2_all['rollover_id'] == 1, 'academic_year'] = 2022
    display(df_subjects2_all)
    
df_subjects_all = pd.concat([df_subjects2_all, df_subjects_all], ignore_index=True)

# Drop rows already in the system (the ones used as template)
df_subjects_all.drop(df_subjects_all[df_subjects_all.school_id == template_school_id].index, inplace=True)

# %%
# %%time

if sis_export_data_to_excel == True:
    
    # Write to Excel the sample template data
    with pd.ExcelWriter(data_directory + '/' + country + '/sis-subject-template-sample-data.xlsx') as writer:        

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
            
     # Write to Excel the data for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-subject-to-insert-data.xlsx') as writer:        

        print("Saving subject with all final data records to Excel")
        df_subjects_all.to_excel(writer, index=False, sheet_name='subject')
else:
    print("Not exporting data to excel")

# %%
# %%time

# Load all data into the database
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:
        print("Loading subject with all final data")
        df_subjects_all.to_sql('subject', con=conn, index=False, if_exists='append')        
        
    print("All subject configuration imported successfully")
else:
    print("Not loading the data into SQL")

# %%
