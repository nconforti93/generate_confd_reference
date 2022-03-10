import xmlrpc.client
import requests, urllib3, ssl
import sys
import json
import pprint

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Fill in any database host, the user and password for the user
    database_host = 'localhost:4443'  # Enter the IP address of any database node. By default, use port 443
    user = 'admin'  # Enter the username you are connecting with
    pw = 'exasol'  # Enter the password for that user

    # The below step can be skipped for single-node instances
    # This finds the host which is managing all of these requests. It will be used to make the connection
    master_ip = requests.get(f'https://{database_host}/master', verify=False).content.decode("utf-8")
    connection_string = f'https://{user}:{pw}@localhost:4443'

    sslcontext = ssl._create_unverified_context()

    # Establish connection
    conn = xmlrpc.client.ServerProxy(connection_string, context=sslcontext, allow_none=True)

    job_name = 'db_configure'
    # View the required parameters for a given job
    job_details = conn.job_desc(job_name)[1]
    print(job_details)
    for i, k in enumerate(job_details):
        if i == 0:
            print("====================Job Description===============\n")
        elif i == 1:
            print("\n==================Mandatory Parameters==========\n")
        elif i == 2:
            print("\n==================Optional Parameters===========\n")
        elif i == 3:
            print("\n==================Substitute Parameters=========\n")
        elif i == 4:
            print("\n==================Allowed Users=================\n")
        elif i == 5:
            print("\n==================Allowed Groups================\n")

        print(json.dumps(k, indent=4))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
