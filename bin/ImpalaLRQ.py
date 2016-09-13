# Impala Long Running Queries
# -*- coding: iso-8859-15 -*-
# Important : Code is TimeZone relevant ( UTC in this program )

# Imports

import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import time
import sys


# Cluster Information
cm_user = "admin"
cm_pass = "admin"
cm_host_port = "http://192.168.56.101:7180"  # Include http/https Prefix
cluster = "Cloudera%20QuickStart"
service = "impala"
cm_api = "v11"
cacert = False  # Path to ca-cert.pem or Set to False if unavailable

# List Queries Running over n minutes
minutes = 5

# Set to True/False
print_cli = True
write_to_file = True
path_to_file = "testWrite.txt"


# Connect via CM API and retrieve Query Information

try:

    url = cm_host_port + "/api/" + cm_api + "/clusters/" + cluster + "/services/" + service + "/impalaQueries"
    r = requests.get(url, auth=HTTPBasicAuth(cm_user, cm_pass), verify=cacert)

except requests.exceptions.RequestException as e:

    print "Caught during connect " + str(e)
    sys.exit(404)

except:

    print "Caught during Response ", sys.exc_info()[0]
    sys.exit(4)


# Parse JSON Output from CM API and send results

try:
    if write_to_file:
        target = open(path_to_file, "a")
    ejson = json.loads(r.content)
    for item in ejson["queries"]:
        if item["queryState"] != "FINISHED":
            sft = ""
            sql_t = ""
            # Parse Start/Current times and Calculate Difference
            sft = time.strptime(item["startTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
            ft = str(sft.tm_year) + ":" + str(sft.tm_mon) + ":" + str(sft.tm_mday) + " " + \
                str(sft.tm_hour) + ":" + str(sft.tm_min) + ":" + str(sft.tm_sec)
            tsql = time.mktime(time.strptime(ft, "%Y:%m:%d %H:%M:%S"))
            sql_t = datetime.datetime.utcnow()
            sql_ft = sql_t.strftime("%Y:%m:%d %H:%M:%S")
            tnow = time.mktime(time.strptime(sql_ft, "%Y:%m:%d %H:%M:%S"))
            tdiff = (tnow-tsql)/60

            if tdiff > minutes:
                # Print on CLI ( @1 Check if we can Pipe all )
                if print_cli:
                    print item["statement"], ", ", \
                        item["user"], ", ",\
                        item["queryId"], ", ",\
                        item["queryState"], ", ",\
                        item["startTime"], ", ", \
                        str(tdiff) + " Minutes"
                # Print to File
                if write_to_file:
                    target.write("hello")
                    target.write(item["statement"] + ", " + item["user"] + ", " + item["queryId"] +
                                 ", " + item["queryState"] + ", " + item["startTime"] + ", " + str(tdiff)
                                 + ", " + " Minutes")
    if write_to_file:
        target.write("\n\n\n NOTE : All Timestamps are in UTC Time Zone")
        target.close()

except:

    print "Caught Exception", sys.exc_info()[0]
    sys.exit(4)

# End of Code
