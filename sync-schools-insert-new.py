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
# data of schools, or more specifically pre-loading the SIS with all the      #
# schools in the EMIS                                                         #
# The approach taken here is a more direct DataFrame to SQL DB                #
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
        
# SIS config
sis_database = config['sis_database']
sis_tenant_id = config['sis_tenant_id']
sis_user_guid = config['sis_user_guid']
sis_country = config['sis_country']
sis_template_school_id = config['sis_template_school_id']
sis_export_data_to_excel = config['sis_export_data_to_excel']
sis_load_data_to_sql = config['sis_load_data_to_sql']
#sis_field_name = config['sis_field_name']
#sis_lov_name = config['sis_lov_name']
#sis_column_name = config['sis_column_name']

# Config
country = config['country']
datetime = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# MS SQL Server connection
ms_connection_string = """
    Driver={{ODBC Driver 17 for SQL Server}};
    Server={},{};
    Database={};
    authentication=SqlPassword;UID={};PWD={};
    TrustServerCertificate=yes;
    autocommit=True
    """.format(config['emis_server_ip'], config['emis_server_port'], config['emis_database'], config['emis_uid'], config['emis_pwd'])

ms_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": ms_connection_string})
ms_engine = create_engine(ms_connection_url)

# MySQL Connection
mysql_connection_string = "mysql+mysqlconnector://"+config['sis_user']+":"+config['sis_pwd']+"@"+config['sis_host']+":"+config['sis_server_port']+"/"+config['sis_database']
mysql_engine = create_engine(mysql_connection_string)

# %%
# Here we create "template" DataFrames for all the tables of interest.
# those will later on be populated with data and loaded directly inth the SQL DB

query_release_number = """
SELECT `release_number`, `school_id`, `tenant_id`, `created_by`, `created_on`, `release_date`, `updated_by`, `updated_on` 
FROM release_number
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_school_master = """
SELECT `school_id`, `tenant_id`, `alternate_name`, `city`, `country`, `county`, `created_by`, `created_on`, `current_period_ends`, `district`, `division`, `features`, `latitude`, `longitude`, `max_api_checks`, `plan_id`, `school_alt_id`, `school_classification`, `school_district_id`, `school_guid`, `school_internal_id`, `school_level`, `school_name`, `school_state_id`, `state`, `street_address_1`, `street_address_2`, `updated_by`, `updated_on`, `zip`
FROM school_master
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_block = """
SELECT `block_id`, `school_id`, `tenant_id`, `academic_year`, `block_sort_order`, `block_title`, `created_by`, `created_on`, `full_day_minutes`, `half_day_minutes`, `rollover_id`, `updated_by`, `updated_on`
FROM `block`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_dpdown_valuelist = """
SELECT `id`, `created_by`, `created_on`, `lov_code`, `lov_column_value`, `lov_name`, `school_id`, `sort_order`, `tenant_id`, `updated_by`, `updated_on`
FROM `dpdown_valuelist`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_fields_category = """
SELECT `category_id`, `school_id`, `tenant_id`, `created_by`, `created_on`, `hide`, `is_system_category`, `is_system_wide_category`, `module`, `required`, `search`, `sort_order`, `title`, `updated_by`, `updated_on`
FROM `fields_category`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_membership = """
SELECT `membership_id`, `school_id`, `tenant_id`, `created_by`, `created_on`, `description`, `is_active`, `is_superadmin`, `is_system`, `profile`, `profile_type`, `updated_by`, `updated_on`
FROM `membership`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_permission_group = """
SELECT `permission_group_id`, `school_id`, `tenant_id`, `active`, `badgeType`, `badgeValue`, `created_by`, `created_on`, `icon`, `icon_type`, `is_active`, `is_system`, `path`, `permission_group_name`, `short_name`, `sort_order`, `title`, `type`, `updated_by`, `updated_on`
FROM `permission_group`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_school_calendars = """
SELECT `calender_id`, `school_id`, `tenant_id`, `academic_year`, `created_by`, `created_on`, `days`, `default_calender`, `end_date`, `rollover_id`, `session_calendar`, `start_date`, `title`, `updated_by`, `updated_on`, `visible_to_membership_id`
FROM `school_calendars`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_school_detail = """
SELECT `id`, `affiliation`, `associations`, `common_toilet_accessibility`, `comon_toilet_type`, `created_by`, `created_on`, `currently_available`, `date_school_closed`, `date_school_opened`, `electricity`, `email`, `facebook`, `fax`, `female_toilet_accessibility`, `female_toilet_type`, `gender`, `handwashing_available`, `highest_grade_level`, `hygene_education`, `instagram`, `internet`, `linkedin`, `locale`, `lowest_grade_level`, `main_source_of_drinking_water`, `male_toilet_accessibility`, `male_toilet_type`, `name_of_assistant_principal`, `name_of_principal`, `running_water`, `school_id`, `school_logo`, `school_thumbnail_logo`, `soap_and_water_available`, `status`, `telephone`, `tenant_id`, `total_common_toilets`, `total_common_toilets_usable`, `total_female_toilets`, `total_female_toilets_usable`, `total_male_toilets`, `total_male_toilets_usable`, `twitter`, `updated_by`, `updated_on`, `website`, `youtube`
FROM `school_detail`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_student_enrollment_code = """
SELECT `enrollment_code`, `school_id`, `tenant_id`, `academic_year`, `created_by`, `created_on`, `rollover_id`, `short_name`, `sort_order`, `title`, `type`, `updated_by`, `updated_on`
FROM `student_enrollment_code`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_custom_fields = """
SELECT `category_id`, `field_id`, `school_id`, `tenant_id`, `created_by`, `created_on`, `default_selection`, `field_name`, `hide`, `is_system_wide_field`, `module`, `required`, `search`, `select_options`, `sort_order`, `system_field`, `title`, `type`, `updated_by`, `updated_on`
FROM `custom_fields`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_permission_category = """
SELECT `permission_category_id`, `school_id`, `tenant_id`, `created_by`, `created_on`, `enable_add`, `enable_delete`, `enable_edit`, `enable_view`, `is_active`, `path`, `permission_category_name`, `permission_group_id`, `short_code`, `sort_order`, `title`, `type`, `updated_by`, `updated_on`
FROM `permission_category`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_permission_subcategory = """
SELECT `permission_subcategory_id`, `school_id`, `tenant_id`, `created_by`, `created_on`, `enable_add`, `enable_delete`, `enable_edit`, `enable_view`, `is_active`, `is_system`, `path`, `permission_category_id`, `permission_group_id`, `permission_subcategory_name`, `short_code`, `sort_order`, `title`, `type`, `updated_by`, `updated_on`
FROM `permission_subcategory`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)

query_role_permission = """
SELECT `role_permission_id`, `school_id`, `tenant_id`, `can_add`, `can_delete`, `can_edit`, `can_view`, `created_by`, `created_on`, `membership_id`, `permission_category_id`, `permission_group_id`, `permission_subcategory_id`, `updated_by`, `updated_on`
FROM `role_permission`
WHERE tenant_id = '{}' AND school_id = {};
""".format(sis_tenant_id, sis_template_school_id)


templates = {
    'release_number': {'query': query_release_number, 'sql_table': 'release_number', 'df_name': None},
    'school_master': {'query': query_school_master, 'sql_table': 'school_master', 'df_name': None},
    'block': {'query': query_block, 'sql_table': 'block', 'df_name': None},
    'dpdown_valuelist': {'query': query_dpdown_valuelist, 'sql_table': 'dpdown_valuelist', 'df_name': None},
    'fields_category': {'query': query_fields_category, 'sql_table': 'fields_category', 'df_name': None},
    'membership': {'query': query_membership, 'sql_table': 'membership', 'df_name': None},
    'permission_group':  {'query': query_permission_group, 'sql_table': 'permission_group', 'df_name': None},
    'school_calendars': {'query': query_school_calendars, 'sql_table': 'school_calendars', 'df_name': None},
    'school_detail': {'query': query_school_detail, 'sql_table': 'school_detail', 'df_name': None},
    'student_enrollment_code': {'query': query_student_enrollment_code, 'sql_table': 'student_enrollment_code', 'df_name': None},
    'custom_fields': {'query': query_custom_fields, 'sql_table': 'custom_fields', 'df_name': None},
    'permission_category': {'query': query_permission_category, 'sql_table': 'permission_category', 'df_name': None},
    'permission_subcategory': {'query': query_permission_subcategory, 'sql_table': 'permission_subcategory', 'df_name': None},
    'role_permission': {'query': query_role_permission, 'sql_table': 'role_permission', 'df_name': None}
}

with mysql_engine.begin() as conn:
    for k,template in templates.items():    
        df = pd.read_sql_query(sa.text(template['query']), conn)    
        template['df'] = df
        print("{} with {} records".format(template['sql_table'], template['df'].shape[0]))
        display(template['df'].head(3))

# %%
# Prepare all the new schools missing from SIS to be loaded from EMIS

# First get the next school_id and school_detail id to be used.
query_school_master_ids = """
SELECT MAX(`school_id`) as last_school_id
FROM `school_master`
ORDER BY `school_id`;
"""

query_school_detail_ids = """
SELECT MAX(`id`) as last_school_detail_id
FROM `school_detail`
ORDER BY `id`;
"""

with mysql_engine.begin() as conn:
    #df_school_calendars = pd.read_sql_query(sa.text(query_school_calendars), conn)
    result1 = conn.execute(sa.text(query_school_master_ids))
    result2 = conn.execute(sa.text(query_school_detail_ids))
    next_school_id = result1.mappings().first()['last_school_id']+1
    next_school_detail_id = result2.mappings().first()['last_school_detail_id']+1
    print("Next school_id should be {}".format(next_school_id))
    print("Next school_detail id should be {}".format(next_school_detail_id))
    
# Retrieve our list of schools missing from SIS    
# %store -r df_schools_sis_to_insert
df_schools_sis_to_insert = df_schools_sis_to_insert.copy()
df_schools_sis_to_insert.insert(0, 'school_id', range(next_school_id, next_school_id + len(df_schools_sis_to_insert)))
df_schools_sis_to_insert.insert(0, 'school_detail_id', range(next_school_detail_id, next_school_detail_id + len(df_schools_sis_to_insert)))
print("New schools missing from SIS to be loaded from EMIS")
print("and their available columns: {}".format(df_schools_sis_to_insert.columns))
display(df_schools_sis_to_insert)

# %%
# For each template DataFrame set the school_id to NaN
# we will later on the the correct school_id and merely using the DataFrame as templates to fill up the data.
for k,template in templates.items():
    template['df']['school_id'] = np.NaN
    # We leave tenant_id untouch and not using the one from the config. The reason is 
    # the database we are reading the templates from is already filtering that tenant_id
    # and this what is in the data is the right one.
    display(template['df'])

# %%
# One possible implementation for school_master...
school_ids = df_schools_sis_to_insert['school_id'].values

school_masters = []
df_school_master_all = None

for school_id in school_ids:
    #print("Preparing school {}".format(school_id))
    df = templates['school_master']['df'].copy()
    df['school_id'] = school_id    
    school_masters.append(df)

if len(school_masters) > 0:
    df_school_master_all = pd.concat(school_masters, ignore_index=True)
    display(df_school_master_all)

# %%
# Another possible implementation for school_master...
school_ids = df_schools_sis_to_insert['school_id'].values
schools_num = len(school_ids)
print("Number of new schools to insert: {}".format(schools_num))

if df_school_master_all:
    ###############################################################################
    # Prepare the school_master DataFrame for all the schools using the template DataFrame
    ###############################################################################
    df_school_master_all = pd.concat([templates['school_master']['df']]*schools_num, ignore_index=True)

    # Set all the data into the template DataFrame
    df_school_master_all['school_id'] = df_schools_sis_to_insert['school_id']
    df_school_master_all['school_guid'] = [str(uuid.uuid4()) for _ in range(len(df_school_master_all.index))]
    df_school_master_all['city'] = df_schools_sis_to_insert['city']
    df_school_master_all['country'] = sis_country
    df_school_master_all['division'] = df_schools_sis_to_insert['division']
    df_school_master_all['county'] = df_schools_sis_to_insert['county']
    df_school_master_all['district'] = df_schools_sis_to_insert['district']
    df_school_master_all['school_internal_id'] = df_schools_sis_to_insert['school_alt_id']
    df_school_master_all['school_district_id'] = df_schools_sis_to_insert['school_district_id']
    df_school_master_all['school_alt_id'] = df_schools_sis_to_insert['school_alt_id']
    df_school_master_all['school_name'] = df_schools_sis_to_insert['school_name']
    df_school_master_all['school_state_id'] = df_schools_sis_to_insert['school_state_id']
    df_school_master_all['state'] = df_schools_sis_to_insert['state']
    df_school_master_all['street_address_1'] = df_schools_sis_to_insert['street_address_1']
    df_school_master_all['street_address_2'] = df_schools_sis_to_insert['street_address_2']
    df_school_master_all['zip'] = 'N/A'
    df_school_master_all['school_level'] = df_schools_sis_to_insert['school_level']
    df_school_master_all['latitude'] = df_schools_sis_to_insert['latitude']
    df_school_master_all['longitude'] = df_schools_sis_to_insert['longitude']

    print("New SIS schools ready for loading (and their columns): {}".format(df_school_master_all.columns))
    # only show the columns with data to view them all
    display(df_school_master_all[['school_id', 'tenant_id', 'school_alt_id', 
                                  'city', 'country', 'county', 'school_district_id', 'district', 'division', 'latitude', 'longitude', 
                                  'plan_id', 'school_classification',  'school_guid', 'school_internal_id',
                                  'school_level', 'school_name', 'school_state_id', 'state',
                                  'created_by', 'created_on']])

    ###############################################################################
    # Same implementation but for school_detail...
    ###############################################################################
    school_ids = df_schools_sis_to_insert['school_id'].values
    schools_num = len(school_ids)
    print("Number of new schools to insert: {}".format(schools_num))

    # Prepare the school_master DataFrame for all the schools using the template DataFrame
    df_school_detail_all = pd.concat([templates['school_detail']['df']]*schools_num, ignore_index=True)

    # Set all the data into the template DataFrame
    df_school_detail_all['id'] = df_schools_sis_to_insert['school_detail_id']
    df_school_detail_all['school_id'] = df_schools_sis_to_insert['school_id']
    df_school_detail_all['affiliation'] = df_schools_sis_to_insert['affiliation']
    df_school_detail_all['date_school_opened'] = df_schools_sis_to_insert['date_school_opened']
    df_school_detail_all['created_on'] = df_school_master_all['created_on']

    print("New SIS school_detail ready for loading (and their columns): {}".format(df_school_detail_all.columns))
    # only show the columns with data to view them all
    display(df_school_detail_all[
    ['id', 'tenant_id', 'school_id', 'affiliation', 'associations', 
     'date_school_closed', 'date_school_opened', 
     'lowest_grade_level', 'highest_grade_level', 
     'locale', 'name_of_principal',
     'status',
     'created_by', 'created_on',
    ]])

    ###############################################################################
    # All other tables are simpler and can be done merely like this
    ###############################################################################
    row_in_df = len(templates['role_permission']['df'])
    df_role_permission_all = pd.concat([templates['role_permission']['df']]*schools_num, ignore_index=True)
    df_role_permission_all['school_id'] = df_schools_sis_to_insert['school_id'].repeat(row_in_df).reset_index(drop=True)
    #display(df_schools_sis_to_insert['school_id'].repeat(row_in_df).reset_index(drop=True))
    display(df_role_permission_all) 
else:
    print("No data to process")

# %%
school_ids = df_schools_sis_to_insert['school_id'].values
schools_num = len(school_ids)
print("Number of new schools to insert: {}".format(schools_num))

# Go through all the template DataFrame and "expand" them with the schools to insert 
# into SIS data
if df_schools_sis_to_insert.shape[0] > 0:
    for k,v in templates.items():

        if k == 'school_master':
            ###############################################################################
            # Prepare the school_master DataFrame for all the schools using the template DataFrame
            ###############################################################################
            df = pd.concat([v['df']]*schools_num, ignore_index=True)

            # Set all the data into the template DataFrame
            df['school_id'] = df_schools_sis_to_insert['school_id']
            df['school_guid'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
            df['city'] = df_schools_sis_to_insert['city']
            df['country'] = sis_country
            df['division'] = df_schools_sis_to_insert['division']
            df['county'] = df_schools_sis_to_insert['county']
            df['district'] = df_schools_sis_to_insert['district']
            df['school_internal_id'] = df_schools_sis_to_insert['school_alt_id']
            df['school_district_id'] = df_schools_sis_to_insert['school_district_id']
            df['school_alt_id'] = df_schools_sis_to_insert['school_alt_id']
            df['school_name'] = df_schools_sis_to_insert['school_name']
            df['school_state_id'] = df_schools_sis_to_insert['school_state_id']
            df['state'] = df_schools_sis_to_insert['state']
            df['street_address_1'] = df_schools_sis_to_insert['street_address_1']
            df['street_address_2'] = df_schools_sis_to_insert['street_address_2']
            df['zip'] = 'N/A'
            df['school_level'] = df_schools_sis_to_insert['school_level']
            df['latitude'] = df_schools_sis_to_insert['latitude']
            df['longitude'] = df_schools_sis_to_insert['longitude']

            v['df'] = df

        elif k == 'school_detail':
            ###############################################################################
            # Same implementation but for school_detail...
            ###############################################################################

            # Prepare the school_master DataFrame for all the schools using the template DataFrame
            df = pd.concat([v['df']]*schools_num, ignore_index=True)

            # Set all the data into the template DataFrame
            df['id'] = df_schools_sis_to_insert['school_detail_id']
            df['school_id'] = df_schools_sis_to_insert['school_id']
            df['affiliation'] = df_schools_sis_to_insert['affiliation']
            df['date_school_opened'] = df_schools_sis_to_insert['date_school_opened']
            df['created_on'] = df_school_master_all['created_on']

            v['df'] = df
        elif k == 'dpdown_valuelist':
            ###############################################################################
            # Cases where the id is a single integer increment and does not repeat for new schools
            ###############################################################################
            row_in_df = len(v['df'])        
            next_id = v['df']['id'].tail(1).item()+1
            df = pd.concat([v['df']]*schools_num, ignore_index=True)
            df['school_id'] = df_schools_sis_to_insert['school_id'].repeat(row_in_df).reset_index(drop=True)
            df['id'] = range(next_id, next_id + len(df))

            v['df'] = df
        else:
            ###############################################################################
            # All other tables are simpler and can be done merely like this
            ###############################################################################
            row_in_df = len(v['df'])
            df = pd.concat([v['df']]*schools_num, ignore_index=True)
            df['school_id'] = df_schools_sis_to_insert['school_id'].repeat(row_in_df).reset_index(drop=True)
            v['df'] = df

    # Print out to output and excel all the resulting DataFrame        
    for k,template in templates.items(): 
        print(k)
        display(template['df'])

else:
    print("No data to process.")

# %%
# %%time
# Runs for about a minute on a VirtualBox VM in my iMac Intel Core i9
# But I did run out of memory and it was consistently failing. I added some memory in the virtual machine the code was running in.

if sis_export_data_to_excel == True:
    tables = ['release_number','school_master','block','dpdown_valuelist','fields_category','membership','permission_group','school_calendars','school_detail','student_enrollment_code','custom_fields','permission_category','role_permission','permission_subcategory']

    # Write to Excel for a final observation before a direct SQL insertion
    with pd.ExcelWriter('data/'+country+'/sis-schools-to-insert-data.xlsx') as writer:
        # Testing with a single DF into a sheet
        #print("Saving {} with {} records to Excel".format(templates['school_master']['sql_table'], templates['school_master']['df'].shape[0]))                
        #templates['school_master']['df'].to_excel(writer, index=False, sheet_name=templates['school_master']['sql_table'])

        for k,v in templates.items():
            print("Saving {} with {} records to Excel".format(v['sql_table'], v['df'].shape[0]))
            if v['sql_table'] in tables:
                v['df'].to_excel(writer, index=False, sheet_name=v['sql_table'])
else:
    print("Not exporting data to excel")

# %%
# %%time
# All data for 137 new schools load into the SIS in about 5 seconds

# Load all data into the database
sis_load_data_to_sql
if sis_load_data_to_sql == True:
    with mysql_engine.begin() as conn:
        for k,v in templates.items():
            print("Inserting {} records into the SQL table {} of database {}".format(v['df'].shape[0], v['sql_table'], sis_database))
            v['df'].to_sql(v['sql_table'], con=conn, index=False, if_exists='append')

    print("All schools imported successfully")
else:
    print("Not loading the data into SQL")

# %%
