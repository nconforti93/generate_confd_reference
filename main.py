import sys
import requests, urllib3, ssl
from bs4 import BeautifulSoup
import glob, os
import xmlrpc.client
import json
import pprint
import pyexasol


def connect_to_confd(host, port, username, pw):
    connection_string = f'https://{username}:{pw}@{host}:{port}'
    sslcontext = ssl._create_unverified_context()
    return xmlrpc.client.ServerProxy(connection_string, context=sslcontext, allow_none=True)

def connect_to_database(host, port, username, pw):
    return pyexasol.connect(dsn=f'{host}:{port}', user=f'{username}', password=f'{pw}', compression=True)

def get_list_of_jobs(connection):
    return connection.job_list()


def execute_confd_job(connection, job_name, params):
    job_results = connection.job_exec(job_name, {'params': params})

    if job_results['result_desc'] == 'Success':
        return job_results['result_output']
    else:
        sys.exit(f'Error executing confd_job {job_name} with parameters {params}. Error: {job_results["result_desc"]}')

def execute_query(connection, query):
    stmt = connection.execute(query)
    return(stmt.fetchall())


def get_job_details(connection, job_name):
    details = connection.job_desc(job_name)

    if details[0] == True:
        return details[1]
    else:
        sys.exit(f'Error getting job details for job {job_name}')

def update_confd_jobs_dict(confd_job_dict, job_details_dict, counter, job_name, detail_name, version):
    if job_details_dict[counter] is not None:
        if len(job_details[i]) > 0:
            if counter >= 4:
                for val in job_details_dict[counter]:
                    confd_job_dict[job_name][detail_name] = [{
                        'data': val,
                        'versions': [version]
                    }]
            else:
                for key, value in job_details_dict[counter].items():
                    confd_job_dict[job_name][detail_name][key] = [{
                        'data': value,
                        'versions': [version]
                    }]

def compare_and_update_details_1(list1, list2, list2_index, version):
    for index, value in enumerate(list1):
        if list2[list2_index] == value['data']:
            # add new version to the list
            list1[index]['versions'].append(version)
            break
        else:
            if index + 1 == len(list1):
                # Add new entry with the corresponding version
                list1.append(
                    {
                        'data': list2[list2_index],
                        'versions': [version]
                    }
                )
                break

# This is used for the parameters due to different structure
def compare_and_update_details_2(list1, dict2, list2_index, version):
    for index, value in enumerate(list1):
        if dict2 == value['data']:
            # add new version to the list
            list1[index]['versions'].append(version)
            break
        else:
            if index + 1 == len(list1):
                # Add new entry with the corresponding version
                list1.append(
                    {
                        'data': dict2,
                        'versions': [version]
                    }
                )
                break

def clean_up_data(value):
    if value == "(<class 'str'>,)":
        value = ["<class 'str'>"]


if __name__ == '__main__':

    absolute_path_to_files = 'C:\\Docs\Content\Content\ConfD'
    toc_file = 'C:\Docs\Content\Project\TOCs\Combined TOC.fltoc'
    # Note - the below credentials are NOT database users, these are confd users
    databases = [{'host': 'localhost',
                  'confd_port': 4443,
                  'confd_username': 'admin',
                  'confd_pw': 'exasol',
                  'db_port': 9563,
                  'db_username': 'sys',
                  'db_pw':'exasol'},
                 {'host': 'localhost',
                  'confd_port': 4444,
                  'confd_username': 'admin',
                  'confd_pw': 'exasol',
                  'db_port': 9564,
                  'db_username': 'sys',
                  'db_pw': 'exasol'}]

    confd_jobs = {}
    for database in databases:
        # Make initial connection to the database to read data
        conn = connect_to_confd(database['host'], database['confd_port'], database['confd_username'], database['confd_pw'])

        # Save which version is currently in use in the database
        db_conn = connect_to_database(database['host'], database['db_port'], database['db_username'], database['db_pw'])
        version_family = execute_query(db_conn, 'SELECT DBMS_VERSION FROM EXA_SYSTEM_EVENTS ORDER BY MEASURE_TIME ASC LIMIT 1')[0][0][:3]

        for job in get_list_of_jobs(conn):
            job_details = get_job_details(conn, job)
            # if job already is in a previous version, then just add the corresponding version
            if job in confd_jobs:
                confd_jobs[job]['versions'].append(version_family)

                # Go through the various parameters and compare them. If there is a difference, add new entries

                # Compare the descriptions

                compare_and_update_details_1(confd_jobs[job]['description'], job_details, 0, version_family)



                # Compare mandatory parameters

                for key in job_details[1]:
                    if key in confd_jobs[job]['mandatory_params']:
                        # clean up data
                        clean_up_data(job_details[1][key]['type'])
                        # parameter is already present
                        compare_and_update_details_2(confd_jobs[job]['mandatory_params'][key], job_details[1][key], 1, version_family)






            else:
                confd_jobs[job] = {
                    'job_name': job,
                    'versions': [version_family],
                    'description': [
                        {
                            'data': job_details[0],
                            'versions': [version_family]
                        }
                    ],
                    'mandatory_params': {},
                    'optional_params': {},
                    'substitute_params': {},
                    'allowed_users': {},
                    'allowed_groups': {},
                }

                for i in range(1, 6):
                    detail_name = ''
                    if i == 1:
                        detail_name = 'mandatory_params'
                    elif i == 2:
                        detail_name = 'optional_params'
                    elif i == 3:
                        detail_name = 'substitute_params'
                    elif i == 4:
                        detail_name = 'allowed_users'
                    elif i == 5:
                        detail_name = 'allowed_groups'
                    else:
                        sys.exit('this should never be reached')

                    update_confd_jobs_dict(confd_jobs, job_details, i, job, detail_name, version_family)

    for job, params in confd_jobs.items():
        if not (('7.1' in params['versions']) and ('7.0' in params['versions'])):
            #print(params['job_name'])
            continue
    print("test")

# for each database do:
# Use job_list to Iterate through each job. For each job:
# populate data dictionary entries with
# Job Description
# Mandatory Parameters
# Optional Parameters
# Substitute Parameters
# Allowed Users
# Allowed Groups
# Include which version is relevant here as well.

# Create dictionary of soup entries using the above data

# Clear overview snippet
# Clear TOC entries for all jobs

# Write the TOC
# Write the overview file
# write topic files
