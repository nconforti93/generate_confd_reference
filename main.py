import sys
import requests, urllib3, ssl
from bs4 import BeautifulSoup
import glob, os
import xmlrpc.client
import json
import pprint




if __name__ == '__main__':

absolute_path_to_files = 'C:\\Docs\Content\Content\ConfD'
toc_file = 'C:\Docs\Content\Project\TOCs\Combined TOC.fltoc'


# Clear overview snippet
# Clear TOC entries for all jobs

# Connect to a database

# Use job_list to Iterate through each job. For each job:
    # Create File if not exists
    # Add File to TOC
    # Add Job name & Description to overview snippet
    # Parse through job_desc to populate in each topic:
        # Job Description
        # Mandatory Parameters
        # Optional Parameters
        # Substitute Parameters
        # Allowed Users
        # Allowed Groups

    # Write the topic files
# Write the TOC
# Write the overview file