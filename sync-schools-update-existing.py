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
# data of schools                                                             #
# This notebook produces UPDATE statements to update some schools in SIS      #
# based on data from EMIS                                                     #
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
sis_country = config['sis_country']

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

# %%
# Some data configuration/mapping

# List some mappings from columns in EMIS to columsn in SIS

# all or at least most of the below should have the SIS UI changed to the sysVocab
# Hide all the unecessary ID fields and unused fields
school_column_mappings = {
    # EMIS column: SIS column, they are not named exactly has in the EMIS but its DataFrame equivalent herein    
    'school_id': 'school_alt_id', # 'school_internal_id'
    'address1': 'street_address_1',
    'address2': 'street_address_2',
    'village': 'city',
    'district_id': 'school_district_id',
    'district': 'district',
    'electorate_national_id': 'school_state_id',
    'electorate_national': 'state',
    'electorate_local': 'county',    
    'island': 'division',
    'authority': 'affiliation',
    'authority_govt': 'school_classification',
    'registration_date': 'date_school_opened',
    'school_type': 'school_level',
}

# %%
# Load the relevant data from EMIS database
# The lookup values in the dictionary below (i.e. df_lookup_values) shows all the currently 
# supported lookups that can be sync'ed from EMIS to SIS. This can be set in the config.json 
# (currently one by one only).

query_schools = """
SELECT [schNo] AS school_id
      ,[schName] AS school_name	  
      ,[schAddr1] AS address1
      ,[schAddr2] AS address2
	  ,S.iCode AS island_id
	  ,I.iName AS island
      ,S.schVillage AS village
	  ,D.dID AS district_id
	  ,D.dName AS district
	  ,EL.codeCode AS electorate_local_id
	  ,EL.codeDescription AS electorate_local
	  ,EN.codeCode AS electorate_national_id
	  ,EN.codeDescription AS electorate_national
	  ,A.authCode AS authority_id
	  ,A.authName AS authority
	  ,AT.codeCode AS authority_type_id
	  ,AT.codeDescription AS authority_type
	  ,AG.codeCode AS authority_govt_id
	  ,AG.codeDescription AS authority_govt
	  ,ST.stCode AS school_type_id
	  ,ST.stDescription AS school_type	  
      ,[schLat] AS latitude
      ,[schLong] AS longitude
      ,[schClosed] AS closed
      ,[schCloseReason] AS closed_reason
      ,S.[schRegStatusDate] AS registration_date
  FROM [dbo].[Schools] S
  INNER JOIN Islands I ON S.iCode = I.iCode
  INNER JOIN Districts D ON I.iGroup = D.dID
  INNER JOIN Authorities A ON S.schAuth = A.authCode
  INNER JOIN lkpAuthorityType AT ON A.authType = AT.codeCode
  INNER JOIN lkpAuthorityGovt AG ON AT.codeGroup = AG.codeCode
  INNER JOIN lkpElectorateL EL ON S.schElectL = EL.codeCode
  INNER JOIN lkpElectorateN EN ON S.schElectN = EN.codeCode
  INNER JOIN SchoolTypes ST ON S.schType = ST.stCode
"""
query_districts = """SELECT [dID] AS [codeCode], [dName] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[Districts]"""
query_ethnicities = """SELECT [codeCode], [codeDescription], [codeGroup], [codeSeq] FROM [dbo].[lkpEthnicity]"""
query_ethnicity_groups = """SELECT [codeCode], [codeDescription], [codeSeq] FROM [dbo].[lkpEthnicityGroup]"""
query_school_levels = """SELECT [stCode] AS [codeCode], [stDescription] AS [codeDescription], [stSort] AS [codeSeq] FROM [dbo].[SchoolTypes]""" # maps to SchoolTypes in EMIS
query_toilet_types = """SELECT [ttypName] AS [codeCode], [ttypName] AS [codeDescription], [ttypSort] AS [codeSeq] FROM [dbo].[lkpToiletTypes]"""
query_languages = """SELECT [langCode] AS [codeCode], [langName] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[lkpLanguage]"""
query_school_classifications = """SELECT [codeCode] AS [codeCode], [codeDescription] AS [codeDescription], 0 AS [codeSeq] FROM [dbo].[lkpAuthorityGovt]"""
      
with mssql_engine.begin() as conn:
    #df = pd.read_sql_query(sa.text("SELECT 'thing' as txt"), conn)
    df_schools_emis = pd.read_sql_query(sa.text(query_schools), conn)

print("EMIS schools")
print("and their available columns: {}".format(df_schools_emis.columns))
display(df_schools_emis.head(3))

# %%
# Load the School data from SIS database
query_schools = """
SELECT * FROM {}.school_master;
""".format(sis_database)

with mysql_engine.begin() as conn:
    df_schools_sis = pd.read_sql_query(sa.text(query_schools), conn)

display(df_schools_sis.head(3))

# %%
# Check which school is in the EMIS and not in the SIS (missing from SIS)
df_schools_emis2 = df_schools_emis.copy()
df_schools_emis2 = df_schools_emis2[df_schools_emis2['closed'] == 0] # only need opened schools
df_schools_sis2 = df_schools_sis.copy()

# First let's put into a simplified comparable DataFrame
df_schools_emis_name = df_schools_emis2[['school_name']]
#display(df_schools_emis_opened)

df_schools_sis_name = df_schools_sis2[['school_name']]
#display(df_schools_sis_name)

print("Schools both found in EMIS and SIS")
df_common = df_schools_emis_name.merge(df_schools_sis_name,on=['school_name'])
display(df_common)

print("Schools missing from SIS to be added")
df_schools_missing_from_sis = df_schools_emis_name[(~df_schools_emis_name.school_name.isin(df_common.school_name))]
display(df_schools_missing_from_sis)

print("Schools found in SIS but not in EMIS (likely needs a school name correction)")
print("Except for any template/test schools which are normal.")
df_schools_needing_edit_in_sis = df_schools_sis_name[(~df_schools_sis_name.school_name.isin(df_common.school_name))]
display(df_schools_needing_edit_in_sis)

# %%
# We will store the data of missing schools for use in another notebook
df_schools_sis_to_insert = df_schools_emis2.merge(df_schools_missing_from_sis,on=['school_name'])
df_schools_sis_to_insert = df_schools_sis_to_insert.rename(columns=school_column_mappings)
print("SIS schools missing to be inserted")
print("Available columns {}".format(df_schools_sis_to_insert.columns))
display(df_schools_sis_to_insert)

# %store df_schools_sis_to_insert

# %%
# Prepare data to update all existing schools in FedSIS with the official data in EMIS.
# This should be written to be executable at any time to sync updates in a safe way. Ideally for now, the schools
# data is managed in EMIS and sync'ed back.

# Start with the EMIS dataframe and filter out only common schools both in EMIS and SIS
print("EMIS Data with columns mapped to SIS columns")
df_schools_emis2 = df_schools_emis2.rename(columns=school_column_mappings)
display(df_schools_emis2.head(3))

print("SIS school IDs data to update")
df_schools_sis2 = df_schools_sis2[['tenant_id','school_id','school_guid','school_name']]
df_schools_sis2_ids = df_schools_sis2.merge(df_common,on=['school_name'])
display(df_schools_sis2_ids.head(3))

print("SIS schools to update with this data")
df_schools_sis_to_update = df_schools_emis2.merge(df_schools_sis2_ids,on=['school_name'])

# Do some final minor data cleanup/processing
# Except latitude and longitude can not be 'N/A'
df_schools_sis_to_update['latitude'] = df_schools_sis_to_update['latitude'].fillna('NULL')
df_schools_sis_to_update['longitude'] = df_schools_sis_to_update['longitude'].fillna('NULL')
df_schools_sis_to_update = df_schools_sis_to_update.fillna('N/A')

# Add some additional data
df_schools_sis_to_update['country'] = sis_country
df_schools_sis_to_update['updated_by'] = sis_user_guid
df_schools_sis_to_update['updated_on'] = datetime

display(df_schools_sis_to_update.head(3))

schools_sis_to_update = df_schools_sis_to_update.to_dict('records')
schools_sis_to_update[:3]

# %%
# Using the data prepared in previous cell produce the actual SQL script that will UPDATE everything to be in sync.
print("Generating the updates scripts to sync EMIS schools to SIS schools")
filename = 'data/' + country + '/schools-emis-to-sis-update-script.sql'
file = open(filename, "w") 

file.write("USE {};\n\n".format(sis_database))

# Remove all existing ethnicities in the SIS in a rolled back transaction
file.write("START TRANSACTION;\n\n")

# Some basic summary verification
#file.write("SELECT school_id, count(lov_name) num_{} FROM dpdown_valuelist WHERE lov_name = '{}' GROUP BY school_id;\n".format(sis_field_name, sis_lov_name))

# All the UPDATE statements
for r in schools_sis_to_update:
    file.write("UPDATE `school_detail` SET `affiliation` = '{}', `date_school_opened` = '{}', `updated_by` = '{}', `updated_on` = '{}' WHERE school_id = {} AND tenant_id = '{}';\n".format(r['affiliation'], r['date_school_opened'], r['updated_by'], r['updated_on'], r['school_id'], r['tenant_id']));
    file.write("UPDATE `school_master` SET `school_internal_id` = '{}', `school_alt_id` = '{}', `city` = '{}', `street_address_1` = '{}', `street_address_2` = '{}', `division` = '{}', `school_district_id` = '{}', `district` = '{}', `county` = '{}', `school_state_id` = '{}', `state` = '{}', `school_classification` = '{}', `school_level` = '{}', `latitude` = {}, `longitude` = {}, `country` = '{}', `updated_by` = '{}', `updated_on` = '{}' WHERE school_id = {} AND tenant_id = '{}';\n".format(r['school_alt_id'], r['school_alt_id'], r['city'], r['street_address_1'], r['street_address_2'], r['division'], r['school_district_id'], r['district'], r['county'], r['school_state_id'], r['state'], r['school_classification'], r['school_level'], r['latitude'], r['longitude'], r['country'], r['updated_by'], r['updated_on'], r['school_id'], r['tenant_id']));
      
# Some basic summary verification
#file.write("\nSELECT school_id, count(lov_name) num_{} FROM dpdown_valuelist WHERE lov_name = '{}' GROUP BY school_id;\n".format(sis_field_name, sis_lov_name))

# Default to ROLLBACK. Final step is examination of the load script, test in development and then COMMIT when certain.
file.write("\nROLLBACK;")
        
file.close()

# %%
