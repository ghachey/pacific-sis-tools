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
# Pacific EMIS and Pacific SIS. It does the following                         #
# Sync lookup values from Pacific EMIS to Pacific SIS deployments             #
#                                                                             #
# WARNING the the emis_lookups need to be run individually one by one in the  #
# following order to get the IDs correct                                      #
# 1) Choose a emis_lookup
# 2) Run all cells to generate the SQL script
# 3) Execute the SQL script on the DB first with ROLLBACK and when working COMMIT
# 4) Go back to step 1)
#
# TODO better to just run it for all lookups? But doing this requires loading them before
# producing next one and requires a bit of additional work.
###############################################################################

# Core stuff
import os
import json
import datetime as dt

# Data stuff
import pandas as pd # Data analysis
import xlrd # excel 
import pyodbc # SQL DB
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa

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
        
# SIS config
sis_database = config['sis_database']
sis_tenant_id = config['sis_tenant_id']
sis_user_guid = config['sis_user_guid']

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
emis_lookups = {
    'ethnicity': {'sis_field_name': "ethnicity", 'sis_lov_name': "Ethnicity", 'sis_column_name': "ethnicity"},
    'race': {'sis_field_name': "race", 'sis_lov_name': "Race", 'sis_column_name': "race"},
    'school level': {'sis_field_name': "schoolLevel", 'sis_lov_name': "School Level", 'sis_column_name': "school_level"},
    'school classification': {'sis_field_name': "schoolClassification", 'sis_lov_name': "School Classification", 'sis_column_name': "school_classification"},
    'female toilet type': {'sis_field_name': "femaleToiletType", 'sis_lov_name': "Female Toilet Type", 'sis_column_name': "female_toilet_type"},
    'male toilet type': {'sis_field_name': "maleToiletType", 'sis_lov_name': "Male Toilet Type", 'sis_column_name': "male_toilet_type"},
    'common toilet type': {'sis_field_name': "commonToiletType", 'sis_lov_name': "Common Toilet Type", 'sis_column_name': "comon_toilet_type"},
    #'languages': {'sis_field_name': "language", 'sis_lov_name': "Languages", 'sis_column_name': "longuage"}, # Not supported (or currently needed) as it sits in a single table easily editable
}

# Choose the desired lookup to create a sync script for
if emis_lookup == 'ethnicity':
    sis_field_name = "ethnicity"
    sis_lov_name = "Ethnicity"
    sis_column_name = "ethnicity"
elif emis_lookup == 'race':
    sis_field_name = "race"
    sis_lov_name = "Race"
    sis_column_name = "race"
elif emis_lookup == 'school level':
    sis_field_name = "schoolLevel"
    sis_lov_name = "School Level"
    sis_column_name = "school_level"
elif emis_lookup == 'school classification':
    sis_field_name = "schoolClassification"
    sis_lov_name = "School Classification"
    sis_column_name = "school_classification"
elif emis_lookup == 'female toilet type':
    sis_field_name = "femaleToiletType"
    sis_lov_name = "Female Toilet Type"
    sis_column_name = "female_toilet_type"
elif emis_lookup == 'male toilet type':
    sis_field_name = "maleToiletType"
    sis_lov_name = "Male Toilet Type"
    sis_column_name = "male_toilet_type"
elif emis_lookup == 'common toilet type':
    sis_field_name = "commonToiletType"
    sis_lov_name = "Common Toilet Type"
    sis_column_name = "comon_toilet_type"
elif emis_lookup == 'all':
    emis_lookups
else:
    pass

print("Running script for EMIS lookup {}".format(emis_lookup))

# %%
# Load the relevant data from EMIS database
# The lookup values in the dictionary below (i.e. df_lookup_values) shows all the currently 
# supported lookups that can be sync'ed from EMIS to SIS. This can be set in the config.json 
# (currently one by one only).

query_schools = """
SELECT [schNo] AS SCHOOLID
      ,[schName] AS SCHOOLNAME
	  ,I.iName AS ISLAND
	  ,D.dName AS DISTRICT
	  ,A.authName AS AUTHORITY
	  ,AT.codeDescription AS AUTHORITYTYPE
	  ,AG.codeDescription AS URBAN
      ,[schClosed] AS CLOSED
      ,[schCloseReason] AS CLOSEDREASON
  FROM [dbo].[Schools] S
  INNER JOIN Islands I ON S.iCode = I.iCode
  INNER JOIN Districts D ON I.iGroup = D.dID
  INNER JOIN Authorities A ON S.schAuth = A.authCode
  INNER JOIN lkpAuthorityType AT ON A.authType = AT.codeCode
  INNER JOIN lkpAuthorityGovt AG ON AT.codeGroup = AG.codeCode
"""
query_districts = """SELECT [dID] AS [codeCode], [dName] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[Districts]"""
query_ethnicities = """SELECT [codeCode], [codeDescription], [codeGroup], [codeSeq] FROM [dbo].[lkpEthnicity]"""
query_ethnicity_groups = """SELECT [codeCode], [codeDescription], [codeSeq] FROM [dbo].[lkpEthnicityGroup]"""
query_school_levels = """SELECT [stCode] AS [codeCode], [stDescription] AS [codeDescription], [stSort] AS [codeSeq] FROM [dbo].[SchoolTypes]""" # maps to SchoolTypes in EMIS
query_toilet_types = """SELECT [ttypName] AS [codeCode], [ttypName] AS [codeDescription], [ttypSort] AS [codeSeq] FROM [dbo].[lkpToiletTypes]"""
query_languages = """SELECT [langCode] AS [codeCode], [langName] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[lkpLanguage]"""
query_school_classifications = """SELECT [codeCode] AS [codeCode], [codeDescription] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[lkpAuthorityGovt]"""
 
with mssql_engine.begin() as conn:
    df_schools_x = pd.read_sql_query(sa.text(query_schools), conn)
    display(df_schools_x.head(3))

    df_districts = pd.read_sql_query(sa.text(query_districts), conn)
    display(df_districts.head(4))

    df_ethnicities = pd.read_sql_query(sa.text(query_ethnicities), conn)
    display(df_ethnicities.head(3))

    df_ethnicities_groups = pd.read_sql_query(sa.text(query_ethnicity_groups), conn)
    display(df_ethnicities_groups.head(3))

    df_school_levels = pd.read_sql_query(sa.text(query_school_levels), conn)
    display(df_school_levels.head(3))

    df_toilet_types = pd.read_sql_query(sa.text(query_toilet_types), conn)
    display(df_toilet_types.head(3))

    df_languages = pd.read_sql_query(sa.text(query_languages), conn)
    display(df_languages.head(3))

    df_school_classifications = pd.read_sql_query(sa.text(query_school_classifications), conn)
    display(df_school_classifications.head(3))

df_lookup_values = {
    'district': df_districts, # No place to go in the SIS at the moment
    'ethnicity': df_ethnicities,
    'race': df_ethnicities_groups,
    'schoolLevel': df_school_levels,
    'femaleToiletType': df_toilet_types,
    'maleToiletType': df_toilet_types,
    'commonToiletType': df_toilet_types,
    #'languages': df_languages, # Not supported (or currently needed) as it sits in a single table easily editable
    'schoolClassification': df_school_classifications
}

# %%
# Load the core data from SIS database that is always to be loaded
query_schools = """
SELECT * FROM {}.school_master;
""".format(sis_database)

query_dpdown_valuelist_next_id = """
SELECT max(id) as max_id FROM {}.dpdown_valuelist;
""".format(sis_database)

query_fields_category = """
SELECT * FROM {}.fields_category;
""".format(sis_database)

dpdown_valuelist_next_id = None

with mysql_engine.begin() as conn:
    
    result = conn.execute(sa.text(query_dpdown_valuelist_next_id))
    for row in result.mappings():
        print('last dpdown_valielist ID: ', row['max_id'])
        dpdown_valuelist_next_id = row['max_id']
        
    df_schools_sis = pd.read_sql_query(sa.text(query_schools), conn)
    df_fields_category_sis = pd.read_sql_query(sa.text(query_fields_category), conn)

display(df_schools_sis.head(3))    
display(df_fields_category_sis.head(3))

# %%
# Load specific lookup data of interest (just for quick viewing, not needed actually)
query_custom_fields = """
SELECT * FROM {}.custom_fields WHERE field_name = '{}';
""".format(sis_database, sis_field_name)

query_dpdown_valuelist = """
SELECT * FROM {}.dpdown_valuelist WHERE lov_name = '{}';
""".format(sis_database, sis_lov_name)

df_custom_fields = pd.read_sql(query_custom_fields, mysql_engine)
display(df_custom_fields.head(3))

df_dpdown_valuelist = pd.read_sql(query_dpdown_valuelist, mysql_engine)
display(df_dpdown_valuelist.head(3))

# %%
# Load some SIS data (Staff, School, Student, etc.)
query_school_master = """
SELECT * FROM {}.school_master;
""".format(sis_database)

query_school_detail = """
SELECT * FROM {}.school_detail;
""".format(sis_database)

print("SIS school_master")
df_school_master = pd.read_sql(query_school_master, mysql_engine)
display(df_school_master.head(3))

print("SIS school_detail")
df_school_detail = pd.read_sql(query_school_detail, mysql_engine)
display(df_school_detail.head(3))

query_staff_master = """
SELECT * FROM {}.staff_master;
""".format(sis_database)

print("SIS staff_master")
df_staff_master = pd.read_sql(query_staff_master, mysql_engine)
display(df_staff_master.head(3))

query_student_master = """
SELECT * FROM {}.student_master;
""".format(sis_database)

print("SIS student_master")
df_student_master = pd.read_sql(query_student_master, mysql_engine)
display(df_student_master.head(3))

# %%
# Make a simple report to see what will change (what was already in there vs what will be synced)
# Mostly for visual inspection and analysis of possible repercussions.
# Get all associations for lookups values to be removed and generate the necessary update statements to use the new lookups

print("EMIS Lookup")
display(df_lookup_values[sis_field_name])

print("SIS existing values")
df_dpdown_valuelist_distinct = df_dpdown_valuelist[['lov_name','lov_column_value','lov_code']].drop_duplicates()
df_dpdown_valuelist_distinct.rename(columns = {'lov_code':'codeCode', 'lov_column_value':'codeDescription', 'sort_order':'codeSeq'}, inplace = True)
display(df_dpdown_valuelist_distinct)

emis_values = set(df_lookup_values[sis_field_name]['codeDescription'].values)
sis_values = set(df_dpdown_valuelist_distinct['codeDescription'].values)
print("Values in SIS not in EMIS that will need to have its associations cleaned up.")
vals = sis_values - emis_values
print(vals, "\n")

if sis_field_name == 'ethnicity' or sis_field_name == 'race':
    print("Staff with a configured {} that is not in the EMIS {} lookups".format(sis_field_name,sis_field_name))
    df_staff_master_to_clean = df_staff_master[df_staff_master[sis_column_name].isin(vals)][['tenant_id','staff_id','school_id','staff_guid','first_given_name','middle_name','last_family_name',sis_column_name]]
    display(df_staff_master_to_clean)
    staff_master_to_clean = df_staff_master_to_clean.to_dict('records')
    staff_master_to_clean
elif sis_field_name == 'schoolLevel' or sis_field_name == 'schoolClassification':
    print("Schools with a configured {} that is not in the EMIS {} lookups".format(sis_field_name,sis_field_name))
    df_school_master_to_clean = df_school_master[df_school_master[sis_column_name].isin(vals)][['tenant_id','school_id','school_guid','school_internal_id','school_name',sis_column_name]]
    display(df_school_master_to_clean)
    school_master_to_clean = df_school_master_to_clean.to_dict('records')
    school_master_to_clean
elif sis_field_name == 'femaleToiletType' or sis_field_name == 'maleToiletType' or sis_field_name == 'commonToiletType':
    print("Schools (details) with a configured {} that is not in the EMIS {} lookups".format(sis_field_name,sis_field_name))
    df_school_detail_to_clean = df_school_detail[df_school_detail[sis_column_name].isin(vals)][['id','tenant_id','school_id',sis_column_name]]
    display(df_school_detail_to_clean)
    school_detail_to_clean = df_school_detail_to_clean.to_dict('records')
    school_detail_to_clean    
else:
    pass


# %%
# Holds dataframes for any possible EMIS lookup values to be synced

# IMPORTANT - Currently the supported format is an array of tuples with the data (code, description, sort_order)

# The EMIS ethnicities to sync to the SIS
df = df_districts[['codeCode', 'codeDescription', 'codeSeq']]
districts = list(df.itertuples(index=False, name=None))

df = df_ethnicities[['codeCode', 'codeDescription', 'codeSeq']]
ethnicities = list(df.itertuples(index=False, name=None))

df = df_ethnicities_groups[['codeCode', 'codeDescription', 'codeSeq']]
ethnic_groups = list(df.itertuples(index=False, name=None))

df = df_school_levels[['codeCode', 'codeDescription', 'codeSeq']]
school_levels = list(df.itertuples(index=False, name=None))

df = df_toilet_types[['codeCode', 'codeDescription', 'codeSeq']]
toilet_types = list(df.itertuples(index=False, name=None))

df = df_languages[['codeCode', 'codeDescription', 'codeSeq']]
languages = list(df.itertuples(index=False, name=None))

df = df_school_classifications[['codeCode', 'codeDescription', 'codeSeq']]
school_classifications = list(df.itertuples(index=False, name=None))

lookup_values = {
    'district': districts,
    'ethnicity': ethnicities,
    'race': ethnic_groups,
    'schoolLevel': school_levels,    
    'femaleToiletType': toilet_types,
    'maleToiletType': toilet_types,
    'commonToiletType': toilet_types,
    #'languages': languages, # Not supported (or currently needed) as it sits in a single table easily editable
    'schoolClassification': school_classifications
}

# %%
print("Generating inserts script for lookup {}".format(emis_lookup))
# List of existing ids used in dpdown_valuelist. Let's collect for re-use
df_dpdown_valuelist.dropna(subset=['school_id'], inplace=True) # not default values with no school_id though
dpdown_valuelist_ids = list(df_dpdown_valuelist['id'].values)
# The SIS schools
school_ids = list(df_schools_sis['school_id'].values)
# Handling of primary key
next_id = dpdown_valuelist_next_id
# List of invalid values (i.e. not found in EMIS)
invalid_vals = str(tuple(vals)).replace(",)",")").replace("()","('')")

filename = data_directory + '/' + country + '/' + sis_field_name + '-sync-script.sql'
file = open(filename, "w") 

file.write("USE {};\n\n".format(sis_database))

# Remove all existing ethnicities in the SIS in a rolled back transaction
file.write("START TRANSACTION;\n\n")

# Some basic summary verification
file.write("SELECT school_id, count(lov_name) num_{} FROM dpdown_valuelist WHERE lov_name = '{}' GROUP BY school_id;\n".format(sis_field_name, sis_lov_name))

# The following SQL depends on the lookup's module
if sis_field_name == 'ethnicity' or sis_field_name == 'race':
    file.write("SELECT count(staff_guid) tot_staff, {} as invalid_{} FROM staff_master WHERE {} IN {} GROUP BY {};\n\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
    
    # Remove any values that don't have a matching lookups in the EMIS
    for r in staff_master_to_clean:
        file.write("UPDATE staff_master SET {} = NULL WHERE tenant_id = '{}' AND staff_id = {} AND school_id = {} AND staff_guid = '{}';\n".format(sis_column_name,r['tenant_id'],r['staff_id'],r['school_id'],r['staff_guid']))
        
elif sis_field_name == 'schoolLevel' or sis_field_name == 'schoolClassification':
    file.write("SELECT count(school_guid) tot_school, {} as invalid_{} FROM school_master WHERE {} IN {} GROUP BY {};\n\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
    
    # Remove any values that don't have a matching lookups in the EMIS
    for r in school_master_to_clean:
        file.write("#UPDATE school_master SET {} = NULL WHERE tenant_id = '{}' AND school_id = {} AND school_guid = '{}';\n".format(sis_column_name,r['tenant_id'],r['school_id'],r['school_guid']))
        
elif sis_field_name == 'femaleToiletType' or sis_field_name == 'maleToiletType' or sis_field_name == 'commonToiletType':
    file.write("SELECT count(school_id) tot_school_detail, {} as invalid_{} FROM school_detail WHERE {} IN {} GROUP BY {};\n\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
    
    # Remove any values that don't have a matching lookups in the EMIS
    for r in school_detail_to_clean:
        file.write("UPDATE school_detail SET {} = NULL WHERE id = '{}' AND tenant_id = '{}' AND school_id = {};\n".format(sis_column_name,r['id'],r['tenant_id'],r['school_id']))
        
else:
    pass


# The actual deletion
file.write("\n")
file.write("DELETE FROM dpdown_valuelist WHERE lov_name = '{}' AND school_id IS NOT NULL;\n\n".format(sis_lov_name))

# Re-inserts from the EMIS values
for school_id in school_ids:
    # insert statement for all the lookup_values to sync (e.g. ethnicities)
    for lookup in lookup_values[sis_field_name]:
        if len(dpdown_valuelist_ids) != 0:
            # still some ids for re-use
            id = dpdown_valuelist_ids.pop(0)
        else:
            # continue with new IDs
            next_id = next_id + 1
            id = next_id
        insert_str = "INSERT INTO dpdown_valuelist(id, tenant_id, school_id, lov_name, lov_column_value, lov_code, sort_order, created_by, created_on, updated_by, updated_on) VALUES ({}, '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}');\n".format(id, sis_tenant_id, school_id, sis_lov_name, lookup[1], lookup[0], lookup[2], sis_user_guid, datetime, sis_user_guid, datetime)    
        #print(insert_str)
        file.write(insert_str)
        
# Some basic summary verification
file.write("\nSELECT school_id, count(lov_name) num_{} FROM dpdown_valuelist WHERE lov_name = '{}' GROUP BY school_id;\n".format(sis_field_name, sis_lov_name))

if sis_field_name == 'ethnicity' or sis_field_name == 'race':
    file.write("SELECT count(staff_guid) tot_staff, {} as invalid_{} FROM staff_master WHERE {} IN {} GROUP BY {};\n\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
elif sis_field_name == 'schoolLevel' or sis_field_name == 'schoolClassification':
    file.write("SELECT count(school_guid) tot_school, {} as invalid_{} FROM school_master WHERE {} IN {} GROUP BY {};\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
elif sis_field_name == 'femaleToiletType' or sis_field_name == 'maleToiletType' or sis_field_name == 'commonToiletType':
    file.write("SELECT count(school_id) tot_school_detail, {} as invalid_{} FROM school_detail WHERE {} IN {} GROUP BY {};\n\n".format(sis_column_name, sis_column_name, sis_column_name, invalid_vals, sis_column_name))
else:
    pass

# Default to ROLLBACK. Final step is examination of the load script, test in development and then COMMIT when certain.
file.write("\nROLLBACK;")
        
file.close()

# %%
# Close database connection
print("Closing the MySQL connection engine")
mysql_engine.dispose()

# %%
