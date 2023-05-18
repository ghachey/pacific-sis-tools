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
# data of students, or more specifically pre-bulk loading the SIS with all the#
# student in the EMIS. Also capable of skipping existing students             #
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

# The next student_id is a little different then usual as it restarts count based on a school_id
# Easiest for now just to hard code the last record added as template.

# Get the next student IDs for each schools
query_student_id_last_sis = """
SELECT tenant_id, school_id, MAX(student_id) last_student_id
FROM student_master 
GROUP BY tenant_id, school_id 
ORDER BY school_id;
"""

# Get the grade IDs for each schools
query_gradelevels_id_sis = """
SELECT tenant_id, school_id, grade_id, short_name, title
FROM gradelevels
ORDER BY school_id;
"""

# Get the calender IDs
query_calender_id_sis = """
SELECT school_id, MAX(calender_id) as calender_id 
FROM school_calendars 
WHERE default_calender = 1
GROUP BY school_id
ORDER BY school_id;
"""

with mysql_engine.begin() as conn:
    df_last_student_id = pd.read_sql_query(sa.text(query_student_id_last_sis), conn)
    #df_last_student_id['next_student_id'] = df_last_student_id['last_student_id']+1
    print("Last student IDs by schools")
    display(df_last_student_id)
    
    df_gradelevels_id_sis = pd.read_sql_query(sa.text(query_gradelevels_id_sis), conn)
    df_gradelevels_id_sis['school_grade'] = df_gradelevels_id_sis['school_id'].apply(str) + '-' +  df_gradelevels_id_sis['short_name']
    print("Grade IDs by schools")
    display(df_gradelevels_id_sis)
    gradelevels_sis_map = pd.Series(df_gradelevels_id_sis.grade_id.values,index=df_gradelevels_id_sis.school_grade).to_dict()
    gradelevels_title_sis_map = pd.Series(df_gradelevels_id_sis.title.values,index=df_gradelevels_id_sis.school_grade).to_dict()
    
    df_calender_id_sis = pd.read_sql_query(sa.text(query_calender_id_sis), conn)
    print("Calender IDs by schools")
    display(df_calender_id_sis)
    calender_sis_map = pd.Series(df_calender_id_sis.calender_id.values,index=df_calender_id_sis.school_id).to_dict()
    
# Retrieve our list of schools missing from SIS    
# %store -r schools_sis_map
# %store -r countries_sis_map
    
# Here we create "template" DataFrames for all the tables of interest.
# They start empty and will later on be populated with data and loaded directly into the SQL DB

query_student_master = """
SELECT `school_id`, `student_id`, `tenant_id`, `admission_number`, `alert_description`, `alternate_id`, `associationship`, `bus_no`, `country_of_birth`, `created_by`, `created_on`, `critical_alert`, `dentist`, `dentist_phone`, `district_id`, `dob`, `economic_disadvantage`, `eligibility_504`, `enrollment_type`, `estimated_grad_date`, `ethnicity`, `facebook`, `first_given_name`, `first_language_id`, `free_lunch_eligibility`, `gender`, `home_address_city`, `home_address_country`, `home_address_line_one`, `home_address_line_two`, `home_address_state`, `home_address_zip`, `home_phone`, `instagram`, `insurance_company`, `insurance_company_phone`, `is_active`, `last_family_name`, `lep_indicator`, `linkedin`, `mailing_address_city`, `mailing_address_country`, `mailing_address_line_one`, `mailing_address_line_two`, `mailing_address_same_to_home`, `mailing_address_state`, `mailing_address_zip`, `marital_status`, `medical_facility`, `medical_facility_phone`, `middle_name`, `mobile_phone`, `nationality`, `other_govt_issued_number`, `personal_email`, `policy_holder`, `policy_number`, `preferred_name`, `previous_name`, `primary_care_physician`, `primary_care_physician_phone`, `race`, `roll_number`, `salutation`, `school_bus_drop_off`, `school_bus_pick_up`, `school_email`, `second_language_id`, `section_id`, `social_security_number`, `special_education_indicator`, `state_id`, `student_guid`, `student_internal_id`, `student_photo`, `student_portal_id`, `student_thumbnail_photo`, `suffix`, `third_language_id`, `twitter`, `updated_by`, `updated_on`, `vision`, `vision_phone`, `youtube`
FROM student_master
WHERE tenant_id = '{}' AND school_id = {} AND student_id = {};
""".format(sis_tenant_id, 115, 1)

query_student_enrollment = """
SELECT `enrollment_id`, `school_id`, `student_id`, `tenant_id`, `calender_id`, `created_by`, `created_on`, `enrollment_code`, `enrollment_date`, `exit_code`, `exit_date`, `grade_id`, `grade_level_title`, `is_active`, `rolling_option`, `rollover_id`, `school_name`, `school_transferred`, `student_guid`, `transferred_grade`, `transferred_school_id`, `updated_by`, `updated_on`
FROM student_enrollment
WHERE tenant_id = '{}' AND school_id = {} AND student_id = {};
""".format(sis_tenant_id, 115, 1)

templates = {
    'student_master': {'query': query_student_master, 'sql_table': 'student_master', 'df_name': None},
    'student_enrollment': {'query': query_student_enrollment, 'sql_table': 'student_enrollment', 'df_name': None},
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))

# %%
# Here we'll extract from the EMIS all the student most recent enrollments
query_student_emis = """
SELECT stuCardID
	,stuGiven
	,stuMiddleNames
	,stuFamilyName
	,stuDoB
    ,CASE 
		WHEN [stuGender] = 'F' THEN 'Female' 
		WHEN [stuGender] = 'M' THEN 'Male' 
		ELSE 'Other' 
		END [stuGender]
	,stuEthnicity
	,SE.schNo
	,SC.schName
	,stueYear
	,stueClass
	,stueSpEd
FROM [dbo].[Student_] S 
INNER JOIN [dbo].[StudentEnrolment_] SE ON S.stuID = SE.stuID
INNER JOIN [dbo].[Schools] SC ON SE.schNo = SC.schNo
WHERE stueYear = {}
ORDER BY schNo, stueClass;
""".format(emis_school_year)

# and student already in SIS

query_student_sis = """
SELECT `school_id`, `student_id`, `student_internal_id`, `alternate_id`, `first_given_name`, `middle_name`, `last_family_name`, `dob`, `gender`, `race`, `ethnicity`, `country_of_birth`, `nationality`, `special_education_indicator`, `enrollment_type`, `is_active`
FROM student_master
WHERE tenant_id = '{}';
""".format(sis_tenant_id)

with mssql_engine.begin() as conn:
    df_student_emis = pd.read_sql_query(sa.text(query_student_emis), conn)
    print("EMIS students")
    display(df_student_emis)
    
with mysql_engine.begin() as conn:
    df_student_sis = pd.read_sql_query(sa.text(query_student_sis), conn)
    print("SIS students")
    display(df_student_sis)    

# %%
# View all the columns
print("Columns name of the EMIS dataset")
pp.pprint(df_student_emis.columns)

for k,template in templates.items():  
    print("Columns of the {} dataset".format(k))
    pp.pprint(template['df'].columns)

# Leave them , they get ignored later on anyway...
#not_needed_columns = ['', '', '', '']
    
student_column_mappings = {
    # EMIS column: SIS column, they are not named exactly has in the EMIS but its DataFrame equivalent herein    
    'stuCardID': 'alternate_id', # ' and student_internal_id'
    'stuGiven': 'first_given_name',
    'stuMiddleNames': 'middle_name',
    'stuFamilyName': 'last_family_name',
    'stuDoB': 'dob',
    'stuGender': 'gender',
    'stuEthnicity': 'ethnicity',
    'stueSpEd': 'special_education_indicator',
    'schName': 'school_name'
}

df_student_emis = df_student_emis.rename(columns=student_column_mappings)
#df_student_emis = df_student_emis.drop(columns=not_needed_columns)
print("Data from EMIS with mapped column to SIS and dropped unneeded columns")
display(df_student_emis)

# %%
# Check what student record needs to be loaded.

df_student_emis = df_student_emis.copy()

# The below is a better more general solution to the problem at hand than the simpler merge and isin solution
# See https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
# When using all first name, last name and DoB a few hundreds students additional are loaded that would otherwise be removed from the dataframe...better.
df_student_all = df_student_emis.merge(df_student_sis[['first_given_name','last_family_name','dob']].drop_duplicates(), on=['first_given_name','last_family_name','dob'], how='left', indicator=True)
print("EMIS and SIS merged")
display(df_student_all)

df_student_already_loaded = df_student_all[(df_student_all['_merge'] == 'both')]
print("Student in EMIS already in SIS")
display(df_student_already_loaded)

df_student_not_already_loaded = df_student_all[(df_student_all['_merge'] == 'left_only')]
print("Student in EMIS not in SIS")
display(df_student_not_already_loaded)

# %%
df_student_not_already_loaded = df_student_not_already_loaded.copy()

# Check for missing date of birth and duplicate students
print("EMIS student with missing date of birth")
display(df_student_not_already_loaded[df_student_not_already_loaded['dob'].isna()])
print("Duplicate EMIS student based on student's first and last name (should be safe to load them?!)")
display(df_student_not_already_loaded[df_student_not_already_loaded.duplicated(subset=['first_given_name','last_family_name'], keep=False)])

# Create all the missing columns/data

df_student_not_already_loaded['school_id'] = df_student_not_already_loaded['school_name'].map(schools_sis_map)
df_student_not_already_loaded['tenant_id'] = sis_tenant_id
df_student_not_already_loaded['admission_number'] = np.NaN
df_student_not_already_loaded['alert_description'] = np.NaN
df_student_not_already_loaded['associationship'] = np.NaN
df_student_not_already_loaded['bus_no'] = np.NaN
#df_student_not_already_loaded['country_of_birth'] = df_student_not_already_loaded['schName'].map(countries_sis_map)
df_student_not_already_loaded['country_of_birth'] = np.NaN
df_student_not_already_loaded['created_by'] = sis_user_guid
df_student_not_already_loaded['created_on'] = datetime
df_student_not_already_loaded['critical_alert'] = np.NaN
df_student_not_already_loaded['dentist'] = np.NaN
df_student_not_already_loaded['dentist_phone'] = np.NaN
df_student_not_already_loaded['district_id'] = np.NaN
df_student_not_already_loaded['economic_disadvantage'] = np.NaN	
df_student_not_already_loaded['eligibility_504'] = np.NaN
df_student_not_already_loaded['enrollment_type'] = 'Internal'
df_student_not_already_loaded['estimated_grad_date'] = np.NaN
# No need to map since the SIS stored the name and not the ID
#df_student_not_already_loaded['ethnicity'] = df_student_not_already_loaded['ethnicity'].map(_sis_map)
df_student_not_already_loaded['facebook'] = np.NaN
#df_student_not_already_loaded['first_language_id'] = df_student_not_already_loaded['first_language'].map(_sis_map)
#df_student_not_already_loaded['second_language_id'] = df_student_not_already_loaded['second_language'].map(_sis_map)
#df_student_not_already_loaded['third_language_id'] = df_student_not_already_loaded['third_language'].map(_sis_map)
df_student_not_already_loaded['first_language_id'] = np.NaN
df_student_not_already_loaded['second_language_id'] = np.NaN
df_student_not_already_loaded['third_language_id'] = np.NaN
df_student_not_already_loaded['free_lunch_eligibility'] = np.NaN
df_student_not_already_loaded['home_address_city'] = np.NaN
df_student_not_already_loaded['home_address_country'] = np.NaN
df_student_not_already_loaded['home_address_line_one'] = np.NaN
df_student_not_already_loaded['home_address_line_two'] = np.NaN
df_student_not_already_loaded['home_address_state'] = np.NaN
df_student_not_already_loaded['home_address_zip'] = np.NaN
df_student_not_already_loaded['home_phone'] = np.NaN
df_student_not_already_loaded['instagram'] = np.NaN
df_student_not_already_loaded['insurance_company'] = np.NaN
df_student_not_already_loaded['insurance_company_phone'] = np.NaN
df_student_not_already_loaded['is_active'] = 1
df_student_not_already_loaded['lep_indicator'] = np.NaN
df_student_not_already_loaded['linkedin'] = np.NaN
df_student_not_already_loaded['mailing_address_city'] = np.NaN
df_student_not_already_loaded['mailing_address_country'] = np.NaN
df_student_not_already_loaded['mailing_address_line_one'] = np.NaN
df_student_not_already_loaded['mailing_address_line_two'] = np.NaN
df_student_not_already_loaded['mailing_address_same_to_home'] = np.NaN
df_student_not_already_loaded['mailing_address_state'] = np.NaN
df_student_not_already_loaded['mailing_address_zip'] = np.NaN
df_student_not_already_loaded['marital_status'] = np.NaN
df_student_not_already_loaded['medical_facility'] = np.NaN
df_student_not_already_loaded['medical_facility_phone'] = np.NaN
df_student_not_already_loaded['mobile_phone'] = np.NaN
#df_student_not_already_loaded['nationality'] = df_student_not_already_loaded['nationality'].map(_sis_map)
df_student_not_already_loaded['nationality'] = np.NaN
df_student_not_already_loaded['other_govt_issued_number'] = np.NaN
df_student_not_already_loaded['personal_email'] = np.NaN
df_student_not_already_loaded['policy_holder'] = np.NaN
df_student_not_already_loaded['policy_number'] = np.NaN
df_student_not_already_loaded['preferred_name'] = np.NaN
df_student_not_already_loaded['previous_name'] = np.NaN
df_student_not_already_loaded['primary_care_physician'] = np.NaN
df_student_not_already_loaded['primary_care_physician_phone'] = np.NaN
#df_student_not_already_loaded['race'] = df_student_not_already_loaded['race'].map(_sis_map)
df_student_not_already_loaded['race'] = np.NaN
df_student_not_already_loaded['roll_number'] = np.NaN
df_student_not_already_loaded['salutation'] = np.NaN
df_student_not_already_loaded['school_bus_drop_off'] = np.NaN
df_student_not_already_loaded['school_bus_pick_up'] = np.NaN
df_student_not_already_loaded['school_email'] = np.NaN
df_student_not_already_loaded['second_language_id'] = np.NaN
df_student_not_already_loaded['section_id'] = np.NaN
df_student_not_already_loaded['social_security_number'] = np.NaN
df_student_not_already_loaded['state_id'] = np.NaN
df_student_not_already_loaded['student_guid'] = [str(uuid.uuid4()) for _ in range(len(df_student_not_already_loaded.index))]
df_student_not_already_loaded['student_internal_id'] = df_student_not_already_loaded['alternate_id']
df_student_not_already_loaded['student_photo'] = np.NaN
df_student_not_already_loaded['student_portal_id'] = np.NaN
df_student_not_already_loaded['student_thumbnail_photo'] = np.NaN
df_student_not_already_loaded['suffix'] = np.NaN
df_student_not_already_loaded['twitter'] = np.NaN
df_student_not_already_loaded['updated_by'] = sis_user_guid
df_student_not_already_loaded['updated_on'] = datetime                              
df_student_not_already_loaded['vision'] = np.NaN
df_student_not_already_loaded['vision_phone'] = np.NaN
df_student_not_already_loaded['youtube'] = np.NaN

# Now student_enrollment data
df_student_not_already_loaded['enrollment_id'] = 1
df_student_not_already_loaded['calender_id'] = df_student_not_already_loaded['school_id'].map(calender_sis_map)
df_student_not_already_loaded['enrollment_code'] = 'New'
df_student_not_already_loaded['enrollment_date'] = '2022-09-01'
df_student_not_already_loaded['exit_code'] = np.NaN
df_student_not_already_loaded['exit_date'] = np.NaN

# Grades
df_student_not_already_loaded['school_grade_val'] = df_student_not_already_loaded['school_id'].apply(str) + '-' + df_student_not_already_loaded['stueClass']
df_student_not_already_loaded['grade_id'] = df_student_not_already_loaded['school_grade_val'].map(gradelevels_sis_map)
df_student_not_already_loaded['grade_level_title'] = df_student_not_already_loaded['school_grade_val'].map(gradelevels_title_sis_map)
df_student_not_already_loaded['rolling_option'] = 'Next grade at current school'
df_student_not_already_loaded['rollover_id'] = np.NaN
df_student_not_already_loaded['school_transferred'] = np.NaN
df_student_not_already_loaded['transferred_grade'] = np.NaN
df_student_not_already_loaded['transferred_school_id'] = np.NaN

# Generate student_id
df_student_not_already_loaded = df_student_not_already_loaded.merge(df_last_student_id[['school_id', 'last_student_id']], on='school_id', how='left', indicator='last_student_exists')
df_student_not_already_loaded['last_student_id'] = df_student_not_already_loaded['last_student_id'].fillna(0)
df_student_not_already_loaded['temp_id'] = df_student_not_already_loaded.groupby('school_id').cumcount()
df_student_not_already_loaded['student_id'] = df_student_not_already_loaded['last_student_id'] + df_student_not_already_loaded['temp_id'] + 1

print("Student not already loaded in SIS")
display(df_student_not_already_loaded)

# %%
# Create the final DataFrames for loading the data

df_student_master_final = df_student_not_already_loaded[
    ['school_id', 'student_id', 'tenant_id', 'admission_number',
       'alert_description', 'alternate_id', 'associationship', 'bus_no',
       'country_of_birth', 'created_by', 'created_on', 'critical_alert',
       'dentist', 'dentist_phone', 'district_id', 'dob',
       'economic_disadvantage', 'eligibility_504', 'enrollment_type',
       'estimated_grad_date', 'ethnicity', 'facebook', 'first_given_name',
       'first_language_id', 'free_lunch_eligibility', 'gender',
       'home_address_city', 'home_address_country', 'home_address_line_one',
       'home_address_line_two', 'home_address_state', 'home_address_zip',
       'home_phone', 'instagram', 'insurance_company',
       'insurance_company_phone', 'is_active', 'last_family_name',
       'lep_indicator', 'linkedin', 'mailing_address_city',
       'mailing_address_country', 'mailing_address_line_one',
       'mailing_address_line_two', 'mailing_address_same_to_home',
       'mailing_address_state', 'mailing_address_zip', 'marital_status',
       'medical_facility', 'medical_facility_phone', 'middle_name',
       'mobile_phone', 'nationality', 'other_govt_issued_number',
       'personal_email', 'policy_holder', 'policy_number', 'preferred_name',
       'previous_name', 'primary_care_physician',
       'primary_care_physician_phone', 'race', 'roll_number', 'salutation',
       'school_bus_drop_off', 'school_bus_pick_up', 'school_email',
       'second_language_id', 'section_id', 'social_security_number',
       'special_education_indicator', 'state_id', 'student_guid',
       'student_internal_id', 'student_photo', 'student_portal_id',
       'student_thumbnail_photo', 'suffix', 'third_language_id', 'twitter',
       'updated_by', 'updated_on', 'vision', 'vision_phone', 'youtube']]
df_student_enrollment_final = df_student_not_already_loaded[
    ['enrollment_id', 'school_id', 'student_id', 'tenant_id', 'calender_id',
       'created_by', 'created_on', 'enrollment_code', 'enrollment_date',
       'exit_code', 'exit_date', 'grade_id', 'grade_level_title', 'is_active',
       'rolling_option', 'rollover_id', 'school_name', 'school_transferred',
       'student_guid', 'transferred_grade', 'transferred_school_id',
       'updated_by', 'updated_on']]

print("All the final DataFrames")
display(df_student_master_final)
display(df_student_enrollment_final)

# %%
# %%time

if sis_export_data_to_excel == True:
    
    # Write to Excel the sample template data
    with pd.ExcelWriter('data/'+country+'/sis-student-template-sample-data.xlsx') as writer:        

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
            
     # Write to Excel the data for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-student-to-insert-data.xlsx') as writer:        

        print("Saving student_master with all final data records to Excel")
        df_student_master_final.to_excel(writer, index=False, sheet_name='student_master')
        print("Saving student_enrollment with all final data records to Excel")
        df_student_enrollment_final.to_excel(writer, index=False, sheet_name='student_enrollment')
else:
    print("Not exporting data to excel")

# %%
# %%time
# All data for +22,000 student missing from SIS in about <1 seconds

# Load all data into the database
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:        
        print("Loading student_master with all final data")
        df_student_master_final.to_sql('student_master', con=conn, index=False, if_exists='append')
        print("Loading student_enrollment with all final data")
        df_student_enrollment_final.to_sql('student_enrollment', con=conn, index=False, if_exists='append')
        
    print("All student imported successfully")
else:
    print("Not loading the data into SQL")

# %%
