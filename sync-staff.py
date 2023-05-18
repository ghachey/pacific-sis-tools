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
# data of staff, or more specifically pre-bulk loading the SIS with all the   #
# staff in the EMIS. Also capable of skipping existing staff                  #
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

# Get the next staff ID
query_staff_id_last_sis = """
SELECT MAX(`staff_id`) as last_staff_id
FROM staff_master
ORDER BY `staff_id`;
"""

# Get the ID for several relayed data
query_schools_sis = """
SELECT `school_id`, `school_name`
FROM school_master;
"""

query_countries_sis = """
SELECT `id`, `name`
FROM country;
"""

with mysql_engine.begin() as conn:
    result1 = conn.execute(sa.text(query_staff_id_last_sis))
    next_staff_id = result1.mappings().first()['last_staff_id']+1
    print("Next staff_id will be {}".format(next_staff_id))    
    
    df_schools_sis = pd.read_sql_query(sa.text(query_schools_sis), conn)
    print("SIS schools for school_id mappings")
    display(df_schools_sis)
    schools_sis_map = pd.Series(df_schools_sis.school_id.values,index=df_schools_sis.school_name).to_dict()
    # %store schools_sis_map
    
    df_countries_sis = pd.read_sql_query(sa.text(query_countries_sis), conn)
    print("SIS countries for mappings")
    display(df_countries_sis)    
    countries_sis_map = pd.Series(df_countries_sis.id.values,index=df_countries_sis.name).to_dict()   
    # %store countries_sis_map
    
# Here we create "template" DataFrames for all the tables of interest.
# They start empty and will later on be populated with data and loaded directly into the SQL DB

query_user_master = """
SELECT `emailaddress`, `school_id`, `tenant_id`, `created_by`, `created_on`, `description`, `is_active`, `is_tenantadmin`, `lang_id`, `last_used_school_id`, `login_attempt_date`, `login_failure_count`, `membership_id`, `name`, `passwordhash`, `updated_by`, `updated_on`, `user_id`
FROM user_master
WHERE tenant_id = '{}' AND user_id = {};
""".format(sis_tenant_id, next_staff_id)

query_staff_master = """
SELECT `staff_id`, `tenant_id`, `alternate_id`, `bus_dropoff`, `bus_no`, `bus_pickup`, `country_of_birth`, `created_by`, `created_on`, `disability_description`, `district_id`, `dob`, `emergency_email`, `emergency_first_name`, `emergency_home_phone`, `emergency_last_name`, `emergency_mobile_phone`, `emergency_work_phone`, `end_date`, `ethnicity`, `facebook`, `first_given_name`, `first_language`, `gender`, `home_address_city`, `home_address_country`, `home_address_line_one`, `home_address_line_two`, `home_address_state`, `home_address_zip`, `home_phone`, `homeroom_teacher`, `instagram`, `is_active`, `job_title`, `joining_date`, `last_family_name`, `linkedin`, `login_email_address`, `mailing_address_city`, `mailing_address_country`, `mailing_address_line_one`, `mailing_address_line_two`, `mailing_address_same_to_home`, `mailing_address_state`, `mailing_address_zip`, `marital_status`, `middle_name`, `mobile_phone`, `nationality`, `office_phone`, `other_govt_issued_number`, `other_grade_level_taught`, `other_subject_taught`, `personal_email`, `physical_disability`, `portal_access`, `preferred_name`, `previous_name`, `primary_grade_level_taught`, `primary_subject_taught`, `profile`, `race`, `relationship_to_staff`, `salutation`, `school_email`, `school_id`, `second_language`, `social_security_number`, `staff_guid`, `staff_internal_id`, `staff_photo`, `staff_thumbnail_photo`, `state_id`, `suffix`, `third_language`, `twitter`, `updated_by`, `updated_on`, `youtube`
FROM staff_master
WHERE tenant_id = '{}' AND staff_id = {};
""".format(sis_tenant_id, next_staff_id)

query_staff_school_info = """
SELECT `created_by`, `created_on`, `end_date`, `membership_id`, `profile`, `school_attached_id`, `school_attached_name`, `school_id`, `staff_id`, `start_date`, `tenant_id`, `updated_by`, `updated_on`
FROM staff_school_info
WHERE tenant_id = '{}' AND staff_id = {};
""".format(sis_tenant_id, next_staff_id)

templates = {
    'user_master': {'query': query_user_master, 'sql_table': 'user_master', 'df_name': None},
    'staff_master': {'query': query_staff_master, 'sql_table': 'staff_master', 'df_name': None},
    'staff_school_info': {'query': query_staff_school_info, 'sql_table': 'staff_school_info', 'df_name': None},
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))

# %%
# Here we'll extract from the EMIS all the staff most recent school appointment data
query_staff_emis = """
SELECT -- From [warehouse].[TeacherLocation]
	   TL.[TID]
      ,TL.[SurveyYear]
      ,TL.[SurveySchNo] SchoolCode
	  ,S.[schName] SchoolName
      ,TL.[SurveyRole] RoleCode	  
      ,TR.[codeDescription] Role
	  -- All the below can be used to populate the other_grades_taught field
      ,[Tpk]
      ,[T00]
      ,[T01]
      ,[T02]
      ,[T03]
      ,[T04]
      ,[T05]
      ,[T06]
      ,[T07]
      ,[T08]
      ,[T09]
      ,[T10]
      ,[T11]
      ,[T12]
      ,[T13]
      ,[T14]
      ,[T15]
      ,[T]
      ,[A]
      ,[X]
      ,[Activities]
      ,[TAMX]
	  -- From TeacherIdentity
      ,[tPayroll]
      ,[tDOB]
      ,CASE 
		WHEN [tSex] = 'F' THEN 'Female' 
		WHEN [tSex] = 'M' THEN 'Male' 
		ELSE 'Other' 
		END [tSex]
      ,[tGiven]
      ,[tMiddleNames]
      ,[tSurname]
      ,[tDatePSAppointed]
      ,[tDatePSClosed]
      ,[tCloseReason]
      ,[tRegisterStatus]
  FROM [warehouse].[TeacherLocation] TL
  INNER JOIN [dbo].[lkpTeacherRole] TR ON TL.SurveyRole = TR.codeCode
  INNER JOIN [dbo].[Schools] S ON TL.SurveySchNo = S.schNo
  INNER JOIN [dbo].[TeacherIdentity] TI ON TL.TID = TI.tID
  WHERE SurveyYear = {}
""".format(emis_school_year)

# and staff already in SIS

query_staff_sis = """
SELECT `staff_id`, `school_id`, `first_given_name`, `middle_name`, `last_family_name`, `alternate_id`, `dob`, `login_email_address`
FROM staff_master;
"""

with mssql_engine.begin() as conn:
    print("EMIS Staff")
    df_staff_emis = pd.read_sql_query(sa.text(query_staff_emis), conn)
    display(df_staff_emis.head(3))
    
with mysql_engine.begin() as conn:
    print("SIS Staff")
    df_staff_sis = pd.read_sql_query(sa.text(query_staff_sis), conn)
    display(df_staff_sis.head(3))    

# %%
# View all the columns
print("Columns name of the EMIS dataset")
pp.pprint(df_staff_emis.columns)

for k,template in templates.items():  
    print("Columns of the {} dataset".format(k))
    pp.pprint(template['df'].columns)

not_needed_columns = [
    'TID', 'SurveyYear', 'SchoolCode', 'RoleCode', 
    'Tpk', 'T00', 'T01', 'T02', 'T03', 'T04', 'T05', 'T06', 'T07', 'T08',
    'T09', 'T10', 'T11', 'T12', 'T13', 'T14', 'T15', 'T', 'A', 'X',
    'Activities', 'tDatePSClosed', 'tCloseReason', 'tRegisterStatus'] # 'TAMX' used for determining membership (teacher vs non-teaching staff)
    
staff_column_mappings = {
    # EMIS column: SIS column, they are not named exactly has in the EMIS but its DataFrame equivalent herein    
    'SchoolName': 'school_attached_name', # 'school_internal_id'
    'Role': 'job_title',
    'tPayroll': 'staff_internal_id',
    'tDOB': 'dob',
    'tSex': 'gender',
    'tGiven': 'first_given_name', # Don't forget to duplicate data to 'name' also
    'tMiddleNames': 'middle_name',
    'tSurname': 'last_family_name',
    'tDatePSAppointed': 'joining_date',
}

df_staff_emis = df_staff_emis.rename(columns=staff_column_mappings)
df_staff_emis = df_staff_emis.drop(columns=not_needed_columns)
print("Data from EMIS with mapped column to SIS and dropped unneeded columns")
display(df_staff_emis)

# %%
# Check what staff record needs to be loaded.

# Let's work only with teachers for now?
df_staff_emis = df_staff_emis[df_staff_emis['TAMX'].isin(['T','M'])].copy()

# The below is a better more general solution to the problem at hand than the simpler merge and isin solution
# See https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
df_staff_all = df_staff_emis.merge(df_staff_sis[['first_given_name','last_family_name']].drop_duplicates(), on=['first_given_name','last_family_name'], how='left', indicator=True)
print("EMIS and SIS merged")
display(df_staff_all)

df_staff_already_loaded = df_staff_all[(df_staff_all['_merge'] == 'both')]
print("Staff in EMIS already in SIS")
display(df_staff_already_loaded)

df_staff_not_already_loaded = df_staff_all[(df_staff_all['_merge'] == 'left_only')]
print("Staff in EMIS not in SIS")
display(df_staff_not_already_loaded)

# %%
df_staff_not_already_loaded = df_staff_not_already_loaded.copy()

# Check for missing date of birth and duplicate teachers
print("EMIS staff with missing date of birth")
display(df_staff_not_already_loaded[df_staff_not_already_loaded['dob'].isna()])
print("Duplicate EMIS staff based on teacher's first and last name (should be safe to load them?!)")
display(df_staff_not_already_loaded[df_staff_not_already_loaded.duplicated(subset=['first_given_name','last_family_name'], keep=False)])

# I think best strategy for unique email with so many unknown is to usefull name (ghislainhachey@example.com). This only works if there are no
# teachers with same first and last name. So need to check this first. And also that no such email exists already in the SIS other this data loading will (should) fail.

# Build emails and make sure all are usable
df_emails_in_use_sis = df_staff_sis['login_email_address'].dropna()
#print("Emails in use")
#display(df_emails_in_use_sis)
    
df_staff_not_already_loaded['login_email_address'] = df_staff_not_already_loaded['first_given_name'].str.lower().str.strip() + df_staff_not_already_loaded['last_family_name'].str.lower().str.strip() + '@example.com'
#print("Newly generated emails")
#display(df_staff_not_already_loaded['login_email_address'])

# Any problematic email? If so, perhaps the easiest is just to manually fix
# in DB before running the final loading to SQL
print("Records with email already in use. Likely just staff already loaded into the SIS")
if df_staff_not_already_loaded[df_staff_not_already_loaded['login_email_address'].isin(df_emails_in_use_sis.tolist())].empty:
    print("None")
else:
    display(df_staff_not_already_loaded[df_staff_not_already_loaded['login_email_address'].isin(df_emails_in_use_sis.tolist())])

# Create all the missing columns

df_staff_not_already_loaded['emailaddress'] = df_staff_not_already_loaded['login_email_address']
df_staff_not_already_loaded['school_id'] = df_staff_not_already_loaded['school_attached_name'].map(schools_sis_map)
df_staff_not_already_loaded['tenant_id'] = sis_tenant_id
df_staff_not_already_loaded['created_by'] = sis_user_guid
df_staff_not_already_loaded['created_on'] = datetime
df_staff_not_already_loaded['description'] = np.NaN
df_staff_not_already_loaded['is_active'] = 1
df_staff_not_already_loaded['is_tenantadmin'] = np.NaN
df_staff_not_already_loaded['lang_id'] = 1
df_staff_not_already_loaded['last_used_school_id'] = np.NaN
df_staff_not_already_loaded['login_attempt_date'] =  np.NaN
df_staff_not_already_loaded['login_failure_count'] = np.NaN
df_staff_not_already_loaded['membership_id'] = 4
df_staff_not_already_loaded['name'] = df_staff_not_already_loaded['first_given_name']
df_staff_not_already_loaded['passwordhash'] = '625F45FEB6DD30645BE90B71B9D46BC2A8F8EBABD7E96343DCCB84D14E9C898B'
df_staff_not_already_loaded['updated_by'] = sis_user_guid
df_staff_not_already_loaded['updated_on'] = datetime
df_staff_not_already_loaded['user_id'] = range(next_staff_id, next_staff_id + len(df_staff_not_already_loaded))
df_staff_not_already_loaded['staff_id'] = df_staff_not_already_loaded['user_id']
df_staff_not_already_loaded['alternate_id'] = np.NaN
df_staff_not_already_loaded['bus_dropoff'] = np.NaN
df_staff_not_already_loaded['bus_no'] = np.NaN
df_staff_not_already_loaded['bus_pickup'] = np.NaN
# Need to pull the country of birth from EMIS but unfortunately need to exposure it, it is in the XML on the enrollment record
# needed something quick here
#df_staff_not_already_loaded['country_of_birth'] = df_staff_not_already_loaded['country'].map(_sis_map)
#df_staff_not_already_loaded['nationality'] = df_staff_not_already_loaded['nationality'].map(_sis_map)
#df_staff_not_already_loaded['ethnicity'] = df_staff_not_already_loaded['ethnicity'].map(_sis_map)
#df_staff_not_already_loaded['race'] = df_staff_not_already_loaded['race'].map(_sis_map)
#df_staff_not_already_loaded['first_language'] = df_staff_not_already_loaded['first_language'].map(_sis_map)
#df_staff_not_already_loaded['second_language'] = df_staff_not_already_loaded['second_language'].map(_sis_map)
#df_staff_not_already_loaded['third_language'] = df_staff_not_already_loaded['third_language'].map(_sis_map)
df_staff_not_already_loaded['country_of_birth'] = np.NaN
df_staff_not_already_loaded['nationality'] = np.NaN
df_staff_not_already_loaded['ethnicity'] = np.NaN
df_staff_not_already_loaded['race'] = np.NaN
df_staff_not_already_loaded['first_language'] = np.NaN
df_staff_not_already_loaded['second_language'] = np.NaN
df_staff_not_already_loaded['third_language'] = np.NaN
df_staff_not_already_loaded['disability_description'] = np.NaN
df_staff_not_already_loaded['district_id'] = np.NaN
df_staff_not_already_loaded['emergency_email'] = np.NaN
df_staff_not_already_loaded['emergency_first_name'] = np.NaN
df_staff_not_already_loaded['emergency_home_phone'] = np.NaN
df_staff_not_already_loaded['emergency_last_name'] = np.NaN
df_staff_not_already_loaded['emergency_mobile_phone'] = np.NaN
df_staff_not_already_loaded['emergency_work_phone'] = np.NaN
df_staff_not_already_loaded['end_date'] = np.NaN
df_staff_not_already_loaded['facebook'] = np.NaN
df_staff_not_already_loaded['linkedin'] = np.NaN
df_staff_not_already_loaded['home_address_city'] = np.NaN
df_staff_not_already_loaded['home_address_country'] = np.NaN
df_staff_not_already_loaded['home_address_line_one'] = np.NaN
df_staff_not_already_loaded['home_address_line_two'] = np.NaN
df_staff_not_already_loaded['home_address_state'] = np.NaN
df_staff_not_already_loaded['home_address_zip'] = np.NaN
df_staff_not_already_loaded['home_phone'] = np.NaN
df_staff_not_already_loaded['homeroom_teacher'] = np.NaN
df_staff_not_already_loaded['instagram'] = np.NaN
df_staff_not_already_loaded['is_active'] = np.NaN
df_staff_not_already_loaded['mailing_address_city'] = np.NaN
df_staff_not_already_loaded['mailing_address_country'] = np.NaN
df_staff_not_already_loaded['mailing_address_line_one'] = np.NaN
df_staff_not_already_loaded['mailing_address_line_two'] = np.NaN
df_staff_not_already_loaded['mailing_address_same_to_home'] = np.NaN
df_staff_not_already_loaded['mailing_address_state'] = np.NaN
df_staff_not_already_loaded['mailing_address_zip'] = np.NaN
df_staff_not_already_loaded['marital_status'] = np.NaN
df_staff_not_already_loaded['middle_name'] = np.NaN
df_staff_not_already_loaded['mobile_phone'] = np.NaN
df_staff_not_already_loaded['office_phone'] = np.NaN
df_staff_not_already_loaded['other_govt_issued_number'] = np.NaN
df_staff_not_already_loaded['other_subject_taught'] = np.NaN
df_staff_not_already_loaded['personal_email'] = np.NaN
df_staff_not_already_loaded['physical_disability'] = np.NaN
df_staff_not_already_loaded['portal_access'] = 1
df_staff_not_already_loaded['preferred_name'] = np.NaN
df_staff_not_already_loaded['previous_name'] = np.NaN
df_staff_not_already_loaded['primary_grade_level_taught'] = np.NaN
df_staff_not_already_loaded['primary_subject_taught'] = np.NaN # ? in EMIS (is it worth the time?)
df_staff_not_already_loaded['profile1'] = 'Classroom Teacher'
df_staff_not_already_loaded['relationship_to_staff'] = np.NaN
df_staff_not_already_loaded['salutation'] = np.NaN
df_staff_not_already_loaded['school_email'] = np.NaN
df_staff_not_already_loaded['social_security_number'] = np.NaN
df_staff_not_already_loaded['staff_guid'] = [str(uuid.uuid4()) for _ in range(len(df_staff_not_already_loaded.index))]
df_staff_not_already_loaded['staff_photo'] = np.NaN
df_staff_not_already_loaded['staff_thumbnail_photo'] = np.NaN
df_staff_not_already_loaded['state_id'] = np.NaN
df_staff_not_already_loaded['suffix'] = np.NaN
df_staff_not_already_loaded['twitter'] = np.NaN
df_staff_not_already_loaded['youtube'] = np.NaN
df_staff_not_already_loaded['profile2'] = 'Teacher'
df_staff_not_already_loaded['school_attached_id'] =  df_staff_not_already_loaded['school_attached_name'].map(schools_sis_map)
df_staff_not_already_loaded['start_date'] = '2022-09-01'

# TODO Derive grade level taught from the teacher's duties in the EMIS
df_staff_not_already_loaded['other_grade_level_taught'] = np.NaN

print("Staff not already loaded in SIS")
display(df_staff_not_already_loaded)

# %%
# Create the final DataFrames for loading the data

df_user_master_final = df_staff_not_already_loaded[
    ['emailaddress', 'school_id', 'tenant_id', 'created_by', 'created_on',
       'description', 'is_active', 'is_tenantadmin', 'lang_id',
       'last_used_school_id', 'login_attempt_date', 'login_failure_count',
       'membership_id', 'name', 'passwordhash', 'updated_by', 'updated_on',
       'user_id']]
df_staff_master_final = df_staff_not_already_loaded[
    ['staff_id', 'tenant_id', 'alternate_id', 'bus_dropoff', 'bus_no',
       'bus_pickup', 'country_of_birth', 'created_by', 'created_on',
       'disability_description', 'district_id', 'dob', 'emergency_email',
       'emergency_first_name', 'emergency_home_phone', 'emergency_last_name',
       'emergency_mobile_phone', 'emergency_work_phone', 'end_date',
       'ethnicity', 'facebook', 'first_given_name', 'first_language', 'gender',
       'home_address_city', 'home_address_country', 'home_address_line_one',
       'home_address_line_two', 'home_address_state', 'home_address_zip',
       'home_phone', 'homeroom_teacher', 'instagram', 'is_active', 'job_title',
       'joining_date', 'last_family_name', 'linkedin', 'login_email_address',
       'mailing_address_city', 'mailing_address_country',
       'mailing_address_line_one', 'mailing_address_line_two',
       'mailing_address_same_to_home', 'mailing_address_state',
       'mailing_address_zip', 'marital_status', 'middle_name', 'mobile_phone',
       'nationality', 'office_phone', 'other_govt_issued_number',
       'other_grade_level_taught', 'other_subject_taught', 'personal_email',
       'physical_disability', 'portal_access', 'preferred_name',
       'previous_name', 'primary_grade_level_taught', 'primary_subject_taught',
       'profile1', 'race', 'relationship_to_staff', 'salutation',
       'school_email', 'school_id', 'second_language',
       'social_security_number', 'staff_guid', 'staff_internal_id',
       'staff_photo', 'staff_thumbnail_photo', 'state_id', 'suffix',
       'third_language', 'twitter', 'updated_by', 'updated_on', 'youtube']]
df_staff_school_info_final = df_staff_not_already_loaded[
    ['created_by', 'created_on', 'end_date', 'membership_id', 'profile2',
       'school_attached_id', 'school_attached_name', 'school_id', 'staff_id',
       'start_date', 'tenant_id', 'updated_by', 'updated_on']]

df_staff_master_final = df_staff_master_final.rename(columns={'profile1': 'profile'})
df_staff_school_info_final = df_staff_school_info_final.rename(columns={'profile2': 'profile'})

print("All the final DataFrames")
display(df_user_master_final)
display(df_staff_master_final)
display(df_staff_school_info_final)

# %%
# %%time

if sis_export_data_to_excel == True:
    
    # Write to Excel the sample template data
    with pd.ExcelWriter('data/'+country+'/sis-staff-template-sample-data.xlsx') as writer:        

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
            
     # Write to Excel the data for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-staff-to-insert-data.xlsx') as writer:        

        print("Saving user_master with all final data records to Excel")
        df_user_master_final.to_excel(writer, index=False, sheet_name='user_master')
        print("Saving staff_master with all final data records to Excel")
        df_staff_master_final.to_excel(writer, index=False, sheet_name='staff_master')
        print("Saving staff_school_info with all final data records to Excel")
        df_staff_school_info_final.to_excel(writer, index=False, sheet_name='staff_school_info')
else:
    print("Not exporting data to excel")

# %%
# %%time
# All data for 1284 teachers missing from SIS in about <1 seconds

# Load all data into the database
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:        
        print("Loading user_master with all final data")
        df_user_master_final.to_sql('user_master', con=conn, index=False, if_exists='append')
        print("Loading staff_master with all final data")
        df_staff_master_final.to_sql('staff_master', con=conn, index=False, if_exists='append')
        print("Loading staff_school_info with all final data")
        df_staff_school_info_final.to_sql('staff_school_info', con=conn, index=False, if_exists='append')
        
    print("All staff imported successfully")
else:
    print("Not loading the data into SQL")

# %%
