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

                    if counter in [1,2]:
                        value = clean_up_data(value)
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
    new_type = []
    if value['type'] not in list_of_options:
        list_of_options.append(value['type'])
    if isinstance(value['type'], str):
        wordlist = value['type']
    elif isinstance(value['type'], list):
        wordlist = '\t'.join(value['type'])
    else:
        sys.exit(f"Type is not a string or list. Data: {value}")

    if 'str' in wordlist:
        new_type.append('string')
    if 'int' in wordlist:
        new_type.append('integer')
    if 'bool' in wordlist:
        new_type.append('boolean')
    if 'tuple' in wordlist:
        new_type.append('tuple')
    if 'dict' in wordlist:
        new_type.append('dict')
    if 'object' in wordlist:
        new_type.append('object')

    value['type'] = new_type
    return value

def check_if_file_exists (file_name):

    if os.path.isfile(file_name):
        return True
    else:
        return False

def create_file(path_to_file, file_name, flare_tag):
    if flare_tag == '':
        print(f"{file_name}: flare_tag is empty")
        file_start_text = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns:MadCap="http://www.madcapsoftware.com/Schemas/MadCap.xsd">
"""
    else:
        file_start_text = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns:MadCap="http://www.madcapsoftware.com/Schemas/MadCap.xsd" MadCap:conditions="{flare_tag}">
"""


    file_start_text = file_start_text + f"""
    <head><title>{file_name} | [%=Exasol Variables.TopicTitle%]</title>
        <meta name="description" content="Learn about the ConfD Job {file_name}." />
        <link href="../Resource/TableStylesheets/SimpleTable.css" rel="stylesheet" MadCap:stylesheetType="table" />
    </head>
    <body>
        <MadCap:concept term="aws;azure;gcp;on-prem" />
        <h1>job_example</h1>
        <div id="automated_description">
        </div>
        <h2>Mandatory Parameters</h2>
        <div id="automated_mandatory_params">
        </div>
        <h2>Optional Parameters</h2>
        <div id="automated_optional_params">
        </div>
        <h2>Substitute Parameters</h2>
        <div id="automated_substitute_params">
        </div>
        <h2>Allowed Users</h2>
        <div id="automated_allowed_users">
        </div>
        <h2>Allowed Groups</h2>
        <div id="automated_allowed_groups">
        </div>
        <h2>Examples</h2>
        <div id="automated_examples">
        </div>
    </body>
</html>"""
    f = open(path_to_file, "x")
    insert_into_file(path_to_file,file_start_text)

def clean_up_conditions(version_list):
    all_versions = ['7.0', '7.1']

    # All versions are mapped, can return an empty string
    if all(version in version_list for version in all_versions):
        return ''
    else:
        condition_list = ''
        for version in all_versions:
            if version in version_list:
                if condition_list == '':
                    condition_list = condition_list + f"Versions.{version.replace('.','-')}"
                else:
                    condition_list = condition_list + f", Versions.{version.replace('.', '-')}"
        return condition_list

def insert_into_file(file_name, text):
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(str(text))

if __name__ == '__main__':
    list_of_options = []
    absolute_path_to_files = 'C:\\Docs\Content\Content\ConfD\\'
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
                if job_details[1] is not None:
                    for key in job_details[1]:
                        if key in confd_jobs[job]['mandatory_params']:
                            compare_and_update_details_2(confd_jobs[job]['mandatory_params'][key], clean_up_data(job_details[1][key]), 1, version_family)

                # Compare optional parameters
                if job_details[2] is not None:
                    for key in job_details[2]:
                        if key in confd_jobs[job]['optional_params']:
                            compare_and_update_details_2(confd_jobs[job]['optional_params'][key], clean_up_data(job_details[2][key]), 2, version_family)

                # Compare substitute parameters
                if job_details[3] is not None:
                    for key in job_details[3]:
                        if key in confd_jobs[job]['substitute_params']:
                            compare_and_update_details_2(confd_jobs[job]['substitute_params'][key], job_details[3][key], 3, version_family)

                # Compare allowed users
                if len(job_details) > 4:
                    if job_details[4] is not None:
                        compare_and_update_details_1(confd_jobs[job]['allowed_users'], job_details, 4, version_family)

                # Compare allowed groups
                    if job_details[5] is not None:
                        compare_and_update_details_1(confd_jobs[job]['allowed_groups'], job_details, 5, version_family)



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
        # Print differences found
        if not (('7.1' in params['versions']) and ('7.0' in params['versions'])):
            print(f'{job}: Job not found in both versions')
            continue

        if len(params['description']) > 1:
            print(f'{job}: Job description does not match')
            print(*params['description'], sep='\n')

        for k, param in params['mandatory_params'].items():
            for i in param:
                if not (('7.1' in i['versions']) and ('7.0' in i['versions'])):
                    print(f'{job}: Mandatory parameter {k} has differences!')

        for k, param in params['optional_params'].items():
            for i in param:
                if not (('7.1' in i['versions']) and ('7.0' in i['versions'])):
                    print(f'{job}: Optional parameter {i} has differences!')

        for k, param in params['substitute_params'].items():
            for i in param:
                if not (('7.1' in i['versions']) and ('7.0' in i['versions'])):
                    print(f'{job}: Substitute parameter {i} has differences!')

        if len(params['allowed_users']) > 1:
            print(f'{job}: Job allowed users does not match:')
            print(*params['allowed_users'], sep='\n')

        if len(params['allowed_groups']) > 1:
            print(f'{job}: Job allowed groups does not match:')
            print(*params['allowed_groups'], sep='\n')
    print("test")

    # Now we begin creating all of the Soup stuff

    for job, details in sorted(confd_jobs.items()):
        # Check if there is already a file for this job name

        full_path = absolute_path_to_files + f'{job}.htm'
        if not check_if_file_exists(full_path):
            create_file(full_path, job, clean_up_conditions(details['versions']))

        with open(full_path, encoding="utf8") as fp:
            soup = BeautifulSoup(fp, "xml")

        # Clear all entries in the specified areas
        for detail_name, detail_info in details.items():
            if detail_name not in ['job_name', 'versions']:
                section = soup.find("div", {"id": f"automated_{detail_name}"})
                section.clear()

                #section_append(generate_xml(detail_name, detail_info), 'xml')

            if detail_name == 'description':
                # Add the new entry
                for i in detail_info:
                    section.append(BeautifulSoup(f'<p MadCap:Conditions="{clean_up_conditions(i["versions"])}">{i["data"]}</p>', "xml"))

            if detail_name == 'mandatory_params':
                pass

            if detail_name == 'optional_params':
                pass

            if detail_name == 'substitute_params':
                pass

            if detail_name == 'allowed_users':
                pass

            if detail_name == 'allowed_groups':
                pass

        insert_into_file(full_path, soup)


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
