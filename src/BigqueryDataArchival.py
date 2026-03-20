""" Python Script for automatic Bigquery data back up's
Backup python script for bq production table.
Deleting snapshot table requires granular permissions.
How ever the permission should be applied to SA if jobs are scheduled.
Note the once snapshot is created it cannot be modified and table with same name cannot be created it can only be deleted / dropped.
"""

from google.cloud import bigquery
import google.auth
import os
import datetime
import csv
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

project_id = 'bq-project-id'
source_dataset_id = 'source_dataset'
target_dataset_id = 'target_dataset_snapshot'
source_table_list = []
target_table_list = []

qstart_date = ''
qend_date = ''
tables = []

# SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
# SERVICE_ACCOUNT_VAR = "BQ_SERVICE_ACCOUNT"
# SERVICE_ACCOUNT_TYPE = "service_account"
#
#
# def _load_credentials_from_var(scopes=None):
# """Loads Google credentials from an environment variable.
# The credentials file must be a service account key.
# """
# account_str = os.environ.get(SERVICE_ACCOUNT_VAR)
# if account_str is None:
# return None, None
# try:
# info = json.loads(account_str)
# except ValueError:
# return None, None
#
# # The type key should indicate that the file is either a service account
# # credentials file or an authorized user credentials file.
# credential_type = info.get("type")
#
# if credential_type == SERVICE_ACCOUNT_TYPE:
# from google.oauth2 import service_account
#
# try:
# credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
# except ValueError:
# return None, None
# return credentials, info.get("project_id")
# else:
# return None, None
#
#
# def _get_bigquery_credentials():
# """Gets credentials from the BQ_SERVICE_ACCOUNT env var else GOOGLE_APPLICATION_CREDENTIALS for file path."""
# creds, proj = _load_credentials_from_var(SCOPES)
# if creds is not None:
# return creds, proj
# return google.auth.default(scopes=SCOPES, quota_project_id=proj)


# expiration = DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY);
def get_quarter_dates(client):
 sql_query = """ select DATE_SUB(LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL -1 QUARTER), QUARTER) + 1, interval 3 MONTH) as qstart_date , 
 LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL -1 QUARTER), QUARTER) as qend_date """
 query_dates = client.query(sql_query)
 results = query_dates.result()
 for r in results:
 global qstart_date, qend_date
 qstart_date = r.qstart_date
 qend_date = r.qend_date
 logging.info("The quarter start date is : {}".format(qstart_date))
 logging.info("The quarter end date is : {}".format(qend_date))


def get_table_list(table_list_full_path):
 with open(table_list_full_path, mode='r') as file:
 csvFile = csv.reader(file)
 header = next(csvFile)
 for table in csvFile:
 print(str(table[0]))
 source_table_list.append(str(table[0]))


def create_tables_copy(client):
 for table in source_table_list:
 copy_table_query = ' CREATE OR REPLACE TABLE ' + project_id + '.' + target_dataset_id + '.' + table + '_copy_' + qstart_date.strftime(
 '%Y%m%d') + '_' + qend_date.strftime(
 '%Y%m%d') + ' LIKE ' + project_id + '.' + source_dataset_id + '.' + table

 logging.info(copy_table_query)
 # Uncomment below statment to create copy tables
 # try:
 # copy_table_list = client.query(copy_table_query)
 # except Exception as e:
 # logging.info(e)


def copy_partition_data(client):
 for table in source_table_list:
 sql_query = 'select table_name, column_name from ' + \
 project_id + '.' + \
 source_dataset_id + '.' + \
 'INFORMATION_SCHEMA.COLUMNS' + \
 ' where table_name = ' + '"' + table + '"' + \
 ' and is_partitioning_column = "YES" '
 # print (sql_query)
 logging.info(sql_query)

 r = client.query(sql_query)
 results = r.result()
 for r in results:
 # print(r.column_name)
 insert_query = 'insert into ' + \
 '`' + project_id + '.' + \
 target_dataset_id + '.' + \
 r.table_name + '_copy_' + qstart_date.strftime('%Y%m%d') + '_' + qend_date.strftime(
 '%Y%m%d') + '`' + \
 ' select * from ' + '`' + project_id + '.' + source_dataset_id + '.' + r.table_name + '`' + ' where DATE(' + \
 r.column_name + ')' + ' between ' + "'" + qstart_date.strftime(
 '%Y-%m-%d') + "'" + ' and ' + "'" + qend_date.strftime('%Y-%m-%d') + "'"

 logging.info(insert_query)
 # Uncomment below to copy data.
 # try:
 # insert_data = client.query(insert_query)
 # results = insert_data.result()
 # except Exception as e:
 # logging.info(e)


def create_snapshot(client):
 for table in source_table_list:
 print(str(table))
 target_table_list.append(str(table) + '_copy_' + qstart_date.strftime(
 '%Y%m%d') + '_' + qend_date.strftime('%Y%m%d'))
 for target_table in target_table_list:
 print(target_table)
 # tab [0:23] will remove _copy_20221001_20221231 from the table name
 # tab [-27:0] will remove _snapshot_20221001_20221231 from the table name
 # check for expiration time
 for table in source_table_list:
 snapshot_query = ' CREATE SNAPSHOT TABLE ' + project_id + '.' + target_dataset_id + '.' + table.rstrip( '_copy_' + qstart_date.strftime(
 '%Y%m%d') + '_' + qend_date.strftime('%Y%m%d')) \
 + '_snapshot_' + qstart_date.strftime(
 '%Y%m%d') + '_' + qend_date.strftime('%Y%m%d') + \
 ' CLONE ' + project_id + '.' + target_dataset_id + '.' + table + '_copy_' + qstart_date.strftime(
 '%Y%m%d') + '_' + qend_date.strftime(
 '%Y%m%d') + \
 " OPTIONS (expiration_timestamp = TIMESTAMP (DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))) " # change to 365 days after testing

 logging.info(snapshot_query)
 # try:
 # create_snapshot = client.query(snapshot_query)
 # results = create_snapshot.result()
 # except Exception as e:
 # logging.info(e)

 # Uncomment below and check if same client object can be used to run below
 # create_snapshot = client.query(snapshot_query)
 # if create_snapshot.errors():
 # raise Exception('Snapshot table was not successfully created in bigQuery')


def drop_copy_tables(client):
 for table in target_table_list:
 drop_query = ' DROP TABLE ' + "`" + project_id + "." + target_dataset_id + "." + str(table) + "`"

 logging.info(drop_query)
 # try:
 # drop_copy_tables = client.query(drop_query)
 # results = drop_copy_tables.result()
 # except Exception as e:
 # logging.info(e)

 # drop_results = client.query(drop_query)


if __name__ == "__main__":
 # Arrange the function calls
 logging.info('Begin Execution')
 try:
 #credentials, project_id_env = _get_bigquery_credentials()
 #client = bigquery.Client(credentials=credentials)
 client = bigquery.Client()
 get_quarter_dates(client)
 # get_table_list()
 table_list_full_path = os.getcwd() + '/Tables.csv'
 get_table_list(table_list_full_path)
 create_tables_copy(client)
 copy_partition_data(client)
 create_snapshot(client)
 drop_copy_tables(client)


 except Exception as e:
 logging.info(e)

 logging.info('End of Execution')

# sql_fullpath = os.getcwd() + '/query.sql'
# render_query(sql_fullpath) use