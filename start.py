import re
import sys

# ################################################################################
# Looping over lists
# ################################################################################

print "hello, this is a test program!!"

'''
for i in range(0, 2):
    print "Loop Value is at %s" % (i+1)

lists = ['a', 'b']
for i in lists:
    print "Loop value is \"%s\"" % i


list_of_lists = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
for lists in list_of_lists:
    # print list
    for x in lists:
         print x
'''


file = open("yarn-site.xml", "r")

flag = False
rm = []
for line in file:
    if flag:
        rm.append(line[line.find(">")+1:line.find("</value>")])
        flag = False
    if re.search("webapp", line):
        flag = True

for c in rm:
    print c


# End Code
