# Yarn Application Runtime
# -*- coding: iso-8859-15 -*-
# Important : Code is TimeZone relevant

# Imports

import requests
import json
import datetime
import time
import sys
import re


# Get Active RM URL


def get_active_rm():
    global rm_url
    rm_url = ""
    conf = open("/etc/hadoop/conf/yarn-site.xml", "r")

    flag = False
    rm = []
    for line in conf:
        if flag:
            rm.append("http://" + line[line.find(">") + 1:line.find("</value>")])
            flag = False
        if re.search("yarn.resourcemanager.webapp.address", line):
            flag = True

    for c in rm:
        r = requests.get(c, data={'key': 'value'})
        message = "This is standby RM. Redirecting to the current active RM"
        if message in str(r.content):
            print(str(c) + " is our Standby Resource Manager")
        else:
            print(str(c) + " is our Active Resource Manager")
            rm_url = str(c)


# Connect to Cluster and retrieve Apps


def get_job_data():
    try:
        r = requests.get(rm_url + '/ws/v1/cluster/apps', data={'key': 'value'})
        # r.json
    except requests.exceptions.RequestException as e:
        print "Caught in Requests " + str(e)
        sys.exit(404)
    except:
        print "Caught Exception", sys.exc_info()[0]
        sys.exit(4)

    # Print whatever fits

    try:

        # Prep HTML File (Optional)

        target = open("YarnLR.html", 'w')
        html = """
            <html>
            <head>
            <script type="text/javascript" src="sortable.js"></script>
            <style>
            /* Sortable tables */
            table.sortable thead {
            background-color:#FF0000;
            color:#ffffff;
            font-weight: italic;
            cursor: default;
            }
            </style>
            <title> Yarn Long Running Apps </title>
            </head>
            <body bgcolor=#EDE7E6>
            <table style="width:100%" class="sortable">
            <thead>
            <tr>
            <th>Application ID </th>
            <th>User</th>
            <th>State</th>
            <th>Type</th>
            <th>Start Time</th>
            <th>Run Time (Days:HH:MM:SS) </th>
            </tr>
            </thead>
            <tbody>
        """
        target.write(html)

        # Check if there are apps being listed

        if r.content == "{\"apps\":null}":
            print "No jobs found on the Cluster, was Yarn restarted recently"
            target.write("</tbody></table><h3>No Jobs on the Cluster!</h3></body></html>")
            target.close()
            sys.exit(2)

        sub1 = ""
        sub2 = ""
        ejson = json.loads(r.content)
        for key in ejson.keys():
            sub1 = ejson[key]
            sub2 = sub1["app"]

        # print "Full App List " + str(sub2)
        # print "\n"

        for lists in sub2:

            app = lists

            if str(app["state"]) == "FINISHED" and str(app["applicationType"]) == "MAPREDUCE":
                appID = str(app["id"])
                user = str(app["user"])
                state = str(app["state"])
                aType = str(app["applicationType"])

                start = str(
                    datetime.datetime.fromtimestamp(float(app["startedTime"]) / 1000.).strftime('%Y-%m-%d %H:%M:%S'))
                finish = str(
                    datetime.datetime.fromtimestamp(float(app["finishedTime"]) / 1000.).strftime('%Y-%m-%d %H:%M:%S'))
                timer = start + ", " + finish

                st_epoch = int((app["startedTime"]) / 1000.)
                en_epoch = int((app["finishedTime"]) / 1000.)
                current = int(time.time())

                # alltime = str(st_epoch) + "," + str(en_epoch) + "," + str(current)
                subtime = current - st_epoch
                m, s = divmod(subtime, 60)
                h, m = divmod(m, 60)
                d, h = divmod(h, 24)

                print appID + ", " + user + ", " + state + ", " + aType + ", " + start + ", " + "%d:%d:%02d:%02d" % \
                                                                                                (d, h, m, s)
                trtd = "</td><td>"

                html2 = "<tr><td>" + appID + trtd + user + trtd + state + trtd + aType + trtd + start + trtd + \
                        "%d:%d:%02d:%02d" % (d, h, m, s) + "</td></tr>"
                target.write(html2)

        target.write("</tbody></table></body></html>")
        target.close()

    except Exception as e:
        print "Caught Exception ", e


print(" \n Getting Active Resource Manager....\n")
get_active_rm()

print("\n Fetching Job Data from : " + rm_url + "\n")
get_job_data()


# End of Code
