import os
from xml.etree import ElementTree

for filename in os.listdir('/dev/conf'):
	with open('/dev/conf/' + filename, 'rt') as f:
		tree = ElementTree.parse(f)

	nameline = line = ""
	n = 1
	output = oozie = hive = names = False
	filter1 = ["mapreduce.job.user.name","hive.server2.authentication","oozie.launcher.action.main.class"]

	for node in tree.iter():
	
		if node.tag == "name"  and node.text in filter1:
		 # line = line + "NAME : " + node.text
		 output = True
		 if node.text == "oozie.launcher.action.main.class":
		  oozie =  True
		 elif node.text == "mapreduce.job.user.name":
		  names = True
		 elif node.text == "hive.server2.authentication":
		  hive = True
		  
		if node.tag == "value" and output:
		 if oozie and node.text == "org.apache.oozie.action.hadoop.HiveMain":
		  # line = line + " OOZIE ACTION # " + node.text + "\n"
		  line = line + node.text
		 elif names:
		  # nameline = " NAME         # " + node.text
		  nameline = node.text
		  names = False
		 elif hive:
		  hive = True
		 else:
		  hive = oozie = names = output = False
		  
		if node.tag == "source" and output and hive:
		 if("/var/run" not in node.text and "/data/" not in node.text and "job.xml" not in node.text and "programatically" not in node.text and "hive-site.xml" != node.text):
			 # line = line + " HIVE1        # " + str(n) + ": " + node.text + "\n"	
			 line = line + node.text
			 n = n + 1
		 
		if node.tag == "property" and output:
		 hive = oozie = names = output = False
		 
	if line != "":
	 print( filename + "," + line + "," + nameline )
	 line =  ""
	 n = 1
	 
