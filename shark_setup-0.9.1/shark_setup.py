#!/usr/bin/env python

###
# This script sets up Shark
# SigmoidAnalytics.com
###

##
# Usage: python shark_setup.py
##

import sys
import os
import subprocess

shark_mem = raw_input("Enter Shark Memory [default 1g]: ")
#hive_home = raw_input("Enter HIVE_HOME [default /root/sigmoid/hive-0.11.0-bin]: ")
#hive_home = raw_input("Enter HIVE_CONF_DIR [ /path/to/hive/conf]: ")
spark_home = raw_input("Enter SPARK_HOME[ /path/to/spark/installation]: ")
hadoop_home =""

if len(shark_mem) == 0:
	shark_mem = "1g"
'''
Not Required for Shark 0.9.1
if len(hive_home) == 0:
	#hive_home = "/root/sigmoid/hive-0.11.0-bin"
	print 'Hive Conf should not be empty'
	sys.exit(0)
'''
if len(spark_home) == 0:
	print "Cannot Proceed without spark home. Exiting.."
	sys.exit(0)

print "Searching for HADOOP_HOME..."

conf_file = open(spark_home + "/conf/spark-env.sh",'r')
for line in conf_file:
	if line.startswith("export HADOOP_HOME"):
		h_tmp = line.split("=")[1].replace("\n","")
		hadoop_home = h_tmp
		break

if len(hadoop_home) == 0:
	print "Couldn't find HADOOP-HOME\n"
	hadoop_home = raw_input("Enter HADOOP_HOME[ /path/to/hadoop]: ")
else:
	print 'HADOOP_HOME Found:' + hadoop_home
#hive_conf = hadoop_home + "/conf"
#hive_conf = hive_home

command = '/sbin/ifconfig eth0 | grep "inet addr:" | cut -d: -f2 | cut -d" " -f1'
output = os.popen(command).read()
output = output.replace("\n","")

master = raw_input("Enter MasterURL[ default SPARK://" + output + ":7077]: ")

if len(master) == 0:
	master = "SPARK://" + output + ":7077"

tach_master = raw_input("Enter Tachyon MasterURL[ default " + output + ":19998]: ")

if len(tach_master) == 0:
	tach_master = output + ":19998"

tach_warehouse = raw_input("Enter Tachyon Warehouse[default /sharktables]: ")
if len(tach_warehouse) == 0:
	tach_warehouse = "/sharktables"

os.system('echo "export SHARK_MASTER_MEM='+ shark_mem +'" > conf/shark-env.sh')
os.system('echo "SPARK_JAVA_OPTS+=-Dspark.kryoserializer.buffer.mb=10 " >> conf/shark-env.sh')
os.system('echo "export SPARK_JAVA_OPTS" >> conf/shark-env.sh')
#os.system('echo "export HIVE_HOME='+ hive_home + '" >> conf/shark-env.sh')
os.system('echo "export HADOOP_HOME=' +  hadoop_home + '" >> conf/shark-env.sh')
os.system('echo "export HIVE_CONF_DIR=' + os.getcwd() +  '/conf" >> conf/shark-env.sh')
os.system('echo "export MASTER='+ master +'" >> conf/shark-env.sh')
os.system('echo "export SPARK_HOME=' + spark_home + '" >> conf/shark-env.sh')
os.system('echo "export TACHYON_MASTER=' + tach_master +  '" >> conf/shark-env.sh')
os.system('echo "export TACHYON_WAREHOUSE_PATH=' + tach_warehouse + '" >> conf/shark-env.sh')
os.system('echo "source ' + spark_home + '/conf/spark-env.sh" >> conf/shark-env.sh')

os.system('chmod +x conf/shark-env.sh')

print 'Configuration file Generated'
print '============================'
os.system("cat conf/shark-env.sh")
print '============================'

print 'Rsyncing with slaves'
slave_file = open(spark_home + "/conf/slaves",'r')
for slave in slave_file:
	if not slave.startswith("#"):
		print 'Rsycing with ' + slave
		os.system("rsync -za " + os.getcwd() + " " + slave.replace("\n","") + ":")


print 'Starting Shark Server'
os.system('./bin/shark --service sharkserver 10000 > log.txt 2>&1 &')



