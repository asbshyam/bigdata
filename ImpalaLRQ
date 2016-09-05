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

# Connect via CM API

try:
    values = {'username': 'admin',
              'password': 'admin'}
    r = requests.get('http://192.168.56.101:7180/api/v11/clusters/Cloudera%20QuickStart/services/impala/impalaQueries',
                     auth=HTTPBasicAuth('admin', 'admin'),
                     verify=False)
    # r.json

except requests.exceptions.RequestException as e:
    print "Caught in Requests " + str(e)
    sys.exit(404)

except:
    print "Caught Exception", sys.exc_info()[0]
    sys.exit(4)

# Parse JSON Output from CM API

try:

    ejson = json.loads(r.content)

    for item in ejson["queries"]:
        # print item
        if item["queryState"] == "FINISHED":

            sft = ""
            sql_t = ""

            sft = time.strptime(item["startTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
            ft = str(sft.tm_year) + ":" + str(sft.tm_mon) + ":" + str(sft.tm_mday) + " " + \
                str(sft.tm_hour) + ":" + str(sft.tm_min) + ":" + str(sft.tm_sec)
            tsql = time.mktime(time.strptime(ft, "%Y:%m:%d %H:%M:%S"))

            sql_t = datetime.datetime.utcnow()
            sql_ft = sql_t.strftime("%Y:%m:%d %H:%M:%S")
            tnow = time.mktime(time.strptime(sql_ft, "%Y:%m:%d %H:%M:%S"))

            tdiff = (tnow-tsql)/60

            if tdiff > 0:
                print item["statement"], ", ", \
                    item["user"], ", ",\
                    item["queryId"], ", ",\
                    item["queryState"], ", ",\
                    item["startTime"], ", ", \
                    str(tdiff) + " Minutes"
                exit(0)

except:
    print "Caught Exception", sys.exc_info()[0]
    sys.exit(4)

# End of Code
