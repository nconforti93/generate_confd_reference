import sys
import requests, urllib3, ssl
from bs4 import BeautifulSoup
from bs4 import NavigableString
import glob, os
import xmlrpc.client
import json
import pprint
import pyexasol
from markdown2 import Markdown


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
    return (stmt.fetchall())


def get_job_details(connection, job_name):
    details = connection.job_desc(job_name)

    if details[0] == True:
        return details[1]
    else:
        sys.exit(f'Error getting job details for job {job_name}')


def update_confd_jobs_dict(confd_job_dict, job_details_dict, counter, job_name, detail_name, version):

    if job_details_dict[counter] is not None:
        if len(job_details[counter]) > 0:
            if type(job_details_dict[counter]) is list:
                confd_job_dict[job_name][detail_name] = []
                for val in job_details_dict[counter]:
                    confd_job_dict[job_name][detail_name].append({
                        'data': val,
                        'versions': [version]
                    })
            else:
                for key, value in job_details_dict[counter].items():

                    if counter in [1, 2]:
                        value = clean_up_data(value)
                    confd_job_dict[job_name][detail_name][key] = [{
                        'data': value,
                        'versions': [version]
                    }]

        else:
            # create an empty example if there are no mandatory parameters and no example present
            if counter == 6:
                if not confd_job_dict[job_name]['mandatory_params'] and len(job_details_dict[6]) == 0:
                    confd_job_dict[job_name][detail_name] = []
                    confd_job_dict[job_name][detail_name].append({
                        'data': {},
                        'versions': [version]
                    })
    else:
        # create empty example if there are no mandatory params or examples
        if counter == 6:
            if not confd_job_dict[job_name]['mandatory_params']:
                confd_job_dict[job_name][detail_name] = []
                confd_job_dict[job_name][detail_name].append({
                    'data': {},
                    'versions': [version]
                })
        print(f'{job_name} and {detail_name} have None value')


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
                        'data': list2[list2_index] if list2[list2_index] is not None else '',
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
    match_found = False
    if 'str' in wordlist:
        new_type.append('string')
        match_found = True
    if 'int' in wordlist:
        new_type.append('integer')
        match_found = True
    if 'bool' in wordlist:
        new_type.append('boolean')
        match_found = True
    if 'tuple' in wordlist:
        new_type.append('tuple')
        match_found = True
    if 'dict' in wordlist:
        new_type.append('dict')
        match_found = True
    if 'object' in wordlist:
        new_type.append('object')
        match_found = True
    if 'list' in wordlist:
        new_type.append('list')
        match_found = True

    if not match_found:
        sys.exit(f'Unmapped data type: {value}')

    value['type'] = new_type
    return value


def check_if_file_exists(file_name):
    if os.path.isfile(file_name):
        return True
    else:
        return False


def create_file(path_to_file, file_name, flare_tag):
    if flare_tag == '':
        file_start_text = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns:MadCap="http://www.madcapsoftware.com/Schemas/MadCap.xsd">
"""
    else:
        file_start_text = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns:MadCap="http://www.madcapsoftware.com/Schemas/MadCap.xsd" MadCap:conditions="{flare_tag}">
"""

    file_start_text = file_start_text + f"""
    <head><title>{file_name} - ConfD | [%=Exasol Variables.TopicTitle%]</title>
        <meta name="description" content="Learn about the ConfD Job {file_name}." />
        <link href="../../Resource/TableStylesheets/Standard.css" rel="stylesheet" MadCap:stylesheetType="table" />
    </head>
    <body>
        <MadCap:concept term="aws;azure;gcp;on-prem" />
        <h1>{file_name}</h1>
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
    insert_into_file(path_to_file, file_start_text)


def clean_up_conditions(version_list):
    all_versions = ['8.2']

    # All versions are mapped, can return an empty string
    if all(version in version_list for version in all_versions):
        return ''
    else:
        condition_list = ''
        for version in all_versions:
            if version in version_list:
                if condition_list == '':
                    condition_list = condition_list + f"Versions.{version.replace('.', '-')}"
                else:
                    condition_list = condition_list + f", Versions.{version.replace('.', '-')}"
        return condition_list


def insert_into_file(file_name, text):
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(str(text))


def number_is_even(num):
    if num % 2 == 0:
        return True
    else:
        return False


def generate_xml(job_name, detail_name, data):
    xml = BeautifulSoup('', 'xml')
    global job_list
    if detail_name == 'description':
        # Add the new entry
        for info in data:
            if additional_short_desc is True:
                markdowner = Markdown(extras=["cuddled-lists", "break-on-newline"])
                html_string = markdowner.convert(info['data'])
                md_object = BeautifulSoup(html_string, 'lxml')
                body = md_object.find('body')

                div_attrs = {'MadCap:conditions': clean_up_conditions(info["versions"])}
                div_tag = xml.new_tag('div', **div_attrs)
                for child in body.findChildren(recursive=False):
                    div_tag.append(child)
                # div_tag.append(body.findChildren(recursive=False))
                xml.append(div_tag)
            else:
                p_attrs = {'MadCap:conditions': clean_up_conditions(info["versions"])}
                p_tag = xml.new_tag('p', **p_attrs)
                p_tag.insert(0, info['data'])
                xml.append(p_tag)
            # job_list = insert_row_into_table(job_list, number_is_even(f), clean_up_conditions(info['versions']), cross_reference(f'{job_name}.htm', job_name), info['data'])

    elif detail_name == 'mandatory_params':
        if not data:
            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no mandatory parameters.')
            xml.append(p_tag)
        else:
            # There is some data
            table = create_table('', True, 'Parameter Name', 'Data Type', 'Description')
            i = 1
            for param_name, param_details in data.items():
                for k in param_details:
                    table = insert_row_into_table(table, number_is_even(i), clean_up_conditions(k['versions']),
                                                  param_name, ', '.join(k['data']['type']), k['data']['desc'])
                    xml.append(table)
                    i += 1

    elif detail_name == 'optional_params':
        if not data:
            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no optional parameters.')
            xml.append(p_tag)
        else:
            table = create_table('', True, 'Parameter Name', 'Data Type', 'Description')
            i = 1
            for param_name, param_details in data.items():
                for k in param_details:
                    table = insert_row_into_table(table, number_is_even(i), clean_up_conditions(k['versions']),
                                                  param_name, ', '.join(k['data']['type']), k['data']['desc'])
                    xml.append(table)
                    i += 1

    elif detail_name == 'substitute_params':
        if not data:
            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no substitute parameters.')
            xml.append(p_tag)
        else:
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'The below table describes what parameters can be substituted for another parameter.')
            xml.append(p_tag)
            table = create_table('', True, 'Parameter Name', 'Substitute Parameter')
            i = 1
            for param_name, param_details in data.items():
                for k in param_details:
                    table = insert_row_into_table(table, number_is_even(i), clean_up_conditions(k['versions']),
                                                  param_name, k['data'][0])
                    xml.append(table)
                    i += 1


    elif detail_name == 'allowed_users':
        if not data:
            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no defined allowed users.')
            xml.append(p_tag)
        else:
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'The following users are allowed to run this job:')
            xml.append(p_tag)

            for k in data:
                if isinstance(k['data'], str):
                    k['data'] = list(k['data'].split(', '))
                if k['data'] == []:
                    p_attrs = {'MadCap:conditions': clean_up_conditions(k["versions"])}
                    p_tag = xml.new_tag('p', **p_attrs)
                    p_tag.insert(0, 'There are no defined allowed users.')
                    xml.append(p_tag)
                else:
                    ul_attrs = {'MadCap:conditions': clean_up_conditions(k["versions"])}
                    html_list = xml.new_tag('ul', **ul_attrs)
                    if isinstance(k['data'], str):
                        k['data'] = list(k['data'].split(', '))
                    for user in k['data']:
                        li = xml.new_tag('li')
                        li.string = user
                        html_list.append(li)

                    xml.append(html_list)

    elif detail_name == 'allowed_groups':
        if not data:
            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no defined allowed groups.')
            xml.append(p_tag)

        else:
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'The following groups are allowed to run this job:')
            xml.append(p_tag)

            for k in data:
                if isinstance(k['data'], str):
                    k['data'] = list(k['data'].split(', '))
                if k['data'] == []:
                    p_attrs = {'MadCap:conditions': clean_up_conditions(k["versions"])}
                    p_tag = xml.new_tag('p', **p_attrs)
                    p_tag.insert(0, 'There are no defined allowed groups.')
                    xml.append(p_tag)
                else:

                    ul_attrs = {'MadCap:conditions': clean_up_conditions(k["versions"])}
                    html_list = xml.new_tag('ul', **ul_attrs)

                    for group in k['data']:
                        li = xml.new_tag('li')
                        li.string = group
                        html_list.append(li)

                    xml.append(html_list)

    elif detail_name == 'examples':
        if not data:
            # Check if there are no parameters, then return empty examples

            # Data is empty, return message
            p_tag = xml.new_tag('p')
            p_tag.insert(0, 'There are no examples.')
            xml.append(p_tag)

        else:
            p_tag = xml.new_tag('p')
            p_tag.insert(0,
                         'The following code snippets show how to use this job using both Python (via XML-RPC) and on the command-line using confd_client.')
            xml.append(p_tag)

            # set up Python code snippet container
            python_dropdown = prepare_dropdown('Python using XML-RPC')

            # set up confd_client code snippet container
            confd_dropdown = prepare_dropdown('Command-line using confd_client')

            for k in data:
                if k and type(k['data']) is dict:
                    # Add example in Python
                    python_dropdown_body = python_dropdown.find('MadCap:dropDownBody')
                    python_dropdown_body.append(
                        create_code_snippet(format_code(job_name, 'python', json.dumps(k['data'])), 'Python',
                                            k["versions"]))
                    confd_dropdown_body = confd_dropdown.find('MadCap:dropDownBody')
                    confd_dropdown_body.append(
                        create_code_snippet(format_code(job_name, 'bash', json.dumps(k['data'])), '', k["versions"]))

                else:
                    sys.exit(f'Example was not empty or a dict in job {job_name}')
            xml.append(python_dropdown)
            xml.append(confd_dropdown)


    else:
        sys.exit(f"Attempted to add xml in a non-valid section: {detail_name}")

    return xml


def prepare_dropdown(header_text):
    xml = BeautifulSoup('', 'xml')

    dropdown = xml.new_tag('MadCap:dropDown')
    dropdown_head = xml.new_tag('MadCap:dropDownHead')

    dropdown_hotspot = xml.new_tag('MadCap:dropDownHotspot')
    dropdown_hotspot.insert(0, NavigableString(header_text))
    dropdown_head.append(dropdown_hotspot)

    dropdown_body = xml.new_tag('MadCap:dropDownBody')

    dropdown.append(dropdown_head)
    dropdown.append(dropdown_body)
    return dropdown


def create_code_snippet(code_example, language, versions):
    code_snippet_attrs = {'MadCap:conditions': clean_up_conditions(versions)}

    code_snippet_body_attrs = {}
    code_snippet_body_attrs['MadCap:useLineNumbers'] = "False"
    code_snippet_body_attrs['MadCap:lineNumberStart'] = "1"
    code_snippet_body_attrs['MadCap:continue'] = "False"
    code_snippet_body_attrs['xml:space'] = "preserve"
    if language != '':
        code_snippet_body_attrs['style'] = f'mc-code-lang: {language}'

    snippet_object = BeautifulSoup('', 'xml')
    code_snippet = snippet_object.new_tag('MadCap:codeSnippet', **code_snippet_attrs)
    code_snippet.append(snippet_object.new_tag('MadCap:codeSnippetCopyButton'))

    code_snippet_body = snippet_object.new_tag('MadCap:codeSnippetBody', **code_snippet_body_attrs)
    code_snippet_body.insert(0, NavigableString(code_example))
    code_snippet.append(code_snippet_body)

    return code_snippet


def format_code(job_name, language, code):
    if code != '{}':

        if language == 'bash':
            if job_name == 'license_upload':
                return 'cat license.xml | confd_client license_upload license: "\\"{< -}\\""'
            else:
                return f"confd_client -c {job_name} -a '{code}'"
        elif language == 'python':
            return f"conn.job_exec('{job_name}', {{'params': {code}}})"
    else:
        if language == 'bash':
            return f"confd_client -c {job_name}"
        elif language == 'python':
            return f"conn.job_exec('{job_name}')"


def create_table(conditions, topic, *args):
    table_attributes = {}
    table_attributes['MadCap:conditions'] = conditions
    if (topic):
        table_attributes['style'] = "mc-table-style: url('../../Resource/TableStylesheets/Standard.css');"
    else:
        # Table is created in a snippet
        table_attributes['style'] = "mc-table-style: url('../../../Resource/TableStylesheets/Standard.css');"
    table_attributes['class'] = 'TableStyle-Standard'
    table_attributes['cellspacing'] = '0'

    table_object = BeautifulSoup('')
    new_tab = table_object.new_tag('table', **table_attributes)
    table_object.insert(0, new_tab)
    # Insert column classes
    for k in args:
        col_attrs = {}
        col_attrs['class'] = 'TableStyle-Standard-Column-Column1'
        new_col = table_object.new_tag('col', **col_attrs)
        table_object.find('table').append(new_col)
    thead = table_object.new_tag('thead')
    table_object.find('table').append(thead)
    tbody = table_object.new_tag('tbody')
    table_object.find('table').append(tbody)

    tr_attrs = {'class': 'TableStyle-Standard-Head-Header1'}
    header_row = table_object.new_tag('tr', **tr_attrs)
    table_object.find('thead').append(header_row)

    for k in args:
        th_attrs = {'class': 'TableStyle-Standard-HeadE-Column1-Header1'}
        th = table_object.new_tag('th', **th_attrs)
        th.insert(0, k)
        table_object.find('tr').append(th)

    return new_tab


def insert_row_into_table(table, even, conditions, *args):
    if even:
        tr_attrs = {'class': 'TableStyle-Standard-Body-Body2'}
        td_attrs = {'class': 'TableStyle-Standard-BodyB-Column1-Body2'}
    else:
        tr_attrs = {'class': 'TableStyle-Standard-Body-Body1'}
        td_attrs = {'class': 'TableStyle-Standard-BodyB-Column1-Body1'}

    tr_attrs['MadCap:conditions'] = conditions
    tr = soup.new_tag('tr', **tr_attrs)

    for arg in args:
        td = soup.new_tag('td', **td_attrs)
        td.insert(0, arg)
        tr.append(td)
    table.find('tbody').append(tr)

    return table


def clear_overview_page():
    with open(overview_file, encoding="utf8") as overview:
        job_list = BeautifulSoup(overview, "xml")

    job_list.find('div', {"id": "job_list"}).clear()

    insert_into_file(overview_file, job_list)


def cross_reference(url, text):
    props = {'href': url}
    xref = soup.new_tag('MadCap:xref', **props)
    xref.string = text
    return xref


def get_overview_file_name(job_name):
    if job_name.startswith('bucket'):
        return f'bucketfs_jobs'
    elif job_name.startswith('db_'):
        return f'db_jobs'
    elif job_name.startswith('group_'):
        return f'group_jobs'
    elif job_name.startswith('infra_'):
        return f'infra_jobs'
    elif job_name.startswith('license_'):
        return f'license_jobs'
    elif job_name.startswith('node'):
        return f'node_jobs'
    elif job_name.startswith('object_volume_'):
        return f'object_volume_jobs'
    elif job_name.startswith('plugin_'):
        return f'plugin_jobs'
    elif job_name.startswith('remote_volume'):
        return f'remote_volume_jobs'
    elif job_name.startswith('st_'):
        return f'storage_jobs'
    elif job_name.startswith('user_'):
        return f'user_jobs'
    else:
        return f'other_jobs'


if __name__ == '__main__':
    list_of_options = []
    absolute_path_to_files = 'C:\Docs\Flare_Projects\Exasol\Content\ConfD\jobs\\'
    toc_file = 'C:\Docs\Flare_Projects\Exasol\Project\TOCs\ConfD.fltoc'
    overview_file = 'C:\Docs\Flare_Projects\Exasol\Content\ConfD\ConfD_Reference.htm'
    snippets_root = 'C:\Docs\Flare_Projects\Exasol\Content\Resources\Snippets\ConfD\\'
    toc_paths = '/Content/ConfD/jobs'
    toc_overview_page_path = '/Content/ConfD'
    overview_pages = [
        f'/Content/ConfD/overview_bucketfs_jobs.htm',
        f'/Content/ConfD/overview_db_jobs.htm',
        f'/Content/ConfD/overview_group_jobs.htm',
        f'/Content/ConfD/overview_infra_jobs.htm',
        f'/Content/ConfD/overview_license_jobs.htm',
        f'/Content/ConfD/overview_node_jobs.htm',
        f'/Content/ConfD/overview_object_volume_jobs.htm',
        f'/Content/ConfD/overview_other_jobs.htm',
        f'/Content/ConfD/overview_plugin_jobs.htm',
        f'/Content/ConfD/overview_remote_volume_jobs.htm',
        f'/Content/ConfD/overview_storage_jobs.htm',
        f'/Content/ConfD/overview_user_jobs.htm'
    ]
    # Note - the below credentials are NOT database users, these are confd users
    databases = [{'host': '34.242.6.155',
                  'confd_port': 20003,
                  'confd_username': 'admin',
                  'confd_pw': 'exasol',
                  'db_host': '34.243.45.78',
                  'db_port': 8563,
                  'db_username': 'sys',
                  'db_pw': 'exasol'},
                 ]

    confd_jobs = {}
    for database in databases:
        print(f"Pulling ConfD information from database at {database['host']}:{database['confd_port']} ")
        # Make initial connection to the database to read data
        conn = connect_to_confd(database['host'], database['confd_port'], database['confd_username'],
                                database['confd_pw'])

        # Save which version is currently in use in the database
        db_conn = connect_to_database(database['db_host'], database['db_port'], database['db_username'],
                                      database['db_pw'])
        version_family = \
        execute_query(db_conn, 'SELECT DBMS_VERSION FROM EXA_SYSTEM_EVENTS ORDER BY MEASURE_TIME ASC LIMIT 1')[0][0][:3]

        for job in get_list_of_jobs(conn):
            job_details = get_job_details(conn, job)
            # if job already is in a previous version, then just add the corresponding version
            if job in confd_jobs:

                confd_jobs[job]['versions'].append(version_family)

                # Go through the various parameters and compare them. If there is a difference, add new entries

                # Compare the descriptions, take the long description (found in 11) over short description
                if job_details[11] is None:
                    compare_and_update_details_1(confd_jobs[job]['description'], job_details, 0, version_family)
                else:
                    compare_and_update_details_1(confd_jobs[job]['description'], job_details, 11, version_family)

                # Compare mandatory parameters
                if job_details[1] is not None:
                    for key in job_details[1]:
                        if key in confd_jobs[job]['mandatory_params']:
                            compare_and_update_details_2(confd_jobs[job]['mandatory_params'][key],
                                                         clean_up_data(job_details[1][key]), 1, version_family)

                # Compare optional parameters
                if job_details[2] is not None:
                    for key in job_details[2]:
                        if key in confd_jobs[job]['optional_params']:
                            compare_and_update_details_2(confd_jobs[job]['optional_params'][key],
                                                         clean_up_data(job_details[2][key]), 2, version_family)

                # Compare substitute parameters
                if job_details[3] is not None:
                    for key in job_details[3]:
                        if key in confd_jobs[job]['substitute_params']:
                            compare_and_update_details_2(confd_jobs[job]['substitute_params'][key], job_details[3][key],
                                                         3, version_family)

                # Compare allowed users
                if len(job_details) > 4:
                    if job_details[4] is not None:
                        compare_and_update_details_1(confd_jobs[job]['allowed_users'], job_details, 4, version_family)

                    # Compare allowed groups
                    if job_details[5] is not None:
                        compare_and_update_details_1(confd_jobs[job]['allowed_groups'], job_details, 5, version_family)

                    # Compare examples
                    if job_details[6] is not None:
                        for key in job_details[6]:
                            if key in confd_jobs[job]['examples']:
                                compare_and_update_details_2(confd_jobs[job]['examples'][key],
                                                             job_details[6][key], 6, version_family)



            else:
                confd_jobs[job] = {
                    'job_name': job,
                    'versions': [version_family],
                    'description': [
                        {
                            'data': job_details[0] if job_details[11] is None else job_details[11],
                            'versions': [version_family]
                        }
                    ],
                    'mandatory_params': {},
                    'optional_params': {},
                    'substitute_params': {},
                    'allowed_users': {},
                    'allowed_groups': {},
                    'examples': {},
                    # Store the short description in extra field if the longer one takes precedence
                    'extra_short_description': [
                        {
                            'data': job_details[0] if job_details[11] is not None else '',
                            'versions': [version_family]
                        }
                    ]
                }

                for i in range(1, 7):
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
                    elif i == 6:
                        detail_name = 'examples'
                    else:
                        sys.exit('this should never be reached')

                    update_confd_jobs_dict(confd_jobs, job_details, i, job, detail_name, version_family)

    # Now we begin creating all of the Soup stuff

    # empty the overview snippets
    global snippets
    snippets = {}
    for file in os.listdir(snippets_root):
        overview_file = os.fsdecode(file)

        with open(f'{snippets_root}\{overview_file}', encoding="utf-8-sig") as overview:
            snippets[os.path.basename(file)] = BeautifulSoup(overview, "xml")
        snippets[os.path.basename(file)].find('body').clear()

        snippets[os.path.basename(file)].find('body').append(create_table('', False, 'Job Name', 'Description'))

    # Prepare new TOC entries
    with open(toc_file, encoding="utf-8-sig") as toc:
        toc_page = BeautifulSoup(toc, "xml")

    for page in overview_pages:
        for entry in toc_page.findAll('TocEntry', {'Link': page}):
            entry.clear()

    f = 1
    for job, details in sorted(confd_jobs.items()):
        print(f"Updating file {job}.htm")
        file_name = f'{get_overview_file_name(job)}.flsnp'

        # Check if there is already a file for this job name

        full_path = absolute_path_to_files + f'{job}.htm'
        if not check_if_file_exists(full_path):
            create_file(full_path, job, clean_up_conditions(details['versions']))

        with open(full_path, encoding="utf-8-sig") as fp:
            soup = BeautifulSoup(fp, "xml")
            global additional_short_desc
        additional_short_desc = False
        if details['extra_short_description'][0]['data'] != '':
            additional_short_desc = True

        for detail_name, detail_info in details.items():

            # Add the description to the overview page
            if detail_name == 'description':
                if not additional_short_desc:
                    for info in detail_info:
                        desc_table = snippets[file_name].find('body').find('table')
                        desc_table = insert_row_into_table(desc_table, True,
                                                           clean_up_conditions(info['versions']),
                                                           cross_reference(f'jobs/{job}.htm', job), info['data'])
                        snippets[file_name].find('body').append(desc_table)
            elif detail_name == 'extra_short_description':
                if additional_short_desc:
                    for info in detail_info:
                        desc_table = snippets[file_name].find('body').find('table')

                        desc_table = insert_row_into_table(desc_table, True,
                                                           clean_up_conditions(info['versions']),
                                                           cross_reference(f'../../../ConfD/jobs/{job}.htm', job),
                                                           info['data'])
                        snippets[file_name].find('body').append(desc_table)

            # Clear all entries in the specified areas

            if detail_name not in ['job_name', 'versions', 'extra_short_description']:
                section = soup.find("div", {"id": f"automated_{detail_name}"})
                section.clear()
                # Adds the entry to the new page as well as the entry to the overview page

                section.append(generate_xml(job, detail_name, detail_info))

        # Insert data into topic page
        insert_into_file(full_path, soup)

        # Add page to appropriate TOC locations
        toc_link = f'{toc_overview_page_path}/overview_{get_overview_file_name(job)}.htm'
        for entry in toc_page.findAll('TocEntry', {'Link': toc_link}):
            toc_props = {}
            toc_props['Title'] = '[%=System.LinkedHeader%]'
            toc_props['Link'] = f'{toc_paths}/{job}.htm'

            if clean_up_conditions(details['versions']) != '':
                toc_props['conditions'] = clean_up_conditions(details['versions'])

            new_toc_entry = soup.new_tag('TocEntry', **toc_props)
            entry.append(new_toc_entry)

        f += 1

        insert_into_file(full_path, soup)

    for snippet, content in snippets.items():
        insert_into_file(f'{snippets_root}/{snippet}', content)
    insert_into_file(toc_file, toc_page)

    print("done")
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
