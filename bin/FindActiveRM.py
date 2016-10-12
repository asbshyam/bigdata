import re
import requests

file = open("/etc/hadoop/conf/yarn-site.xml", "r")

flag = False
rm = []
for line in file:
    if flag:
        rm.append(line[line.find(">")+1:line.find("</value>")])
        flag = False
    if re.search("yarn.resourcemanager.webapp.address", line):
        flag = True

for c in rm:
        c = "http://" + c
        r = requests.get(c, data={'key': 'value'})
        message = "This is standby RM. Redirecting to the current active RM"
        if message in str(r.content):
                print(c + " is our Standby Resource Manager")
        else:
                print(c + " is our Active Resource Manager")
