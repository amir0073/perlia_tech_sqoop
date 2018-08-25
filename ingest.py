#!/usr/bin/python

import sys
import os

def load_table(table_name, column_list, map_reduce_number):
   print 'Drop hdfs sqoop file.'
   os.system("hdfs dfs -rm -r hdfs://sandbox.hortonworks.com:8020/ingest/mysql/performance_schema/" + table_name)
   
   print 'Loading table:' + table_name
   
   sqp_stmt = "sqoop import -Dhadoop.security.credential.provider.path=jceks:/ingest/mysql_password/mysql.jceks " + " --connect jdbc:mysql://127.0.0.1:3306/performance_schema " + " --username root " + " --password-alias mysql.password " + " --m " + map_reduce_number + " --columns " +  column_list + " --table " + table_name + " --target-dir /ingest/mysql/performance_schema/" + table_name + " --fields-terminated-by , " + " --hive-import " + " --hive-overwrite " + " --direct " + " --driver com.mysql.jdbc.Driver " + " --hive-table performance_schema." + table_name + " --package-name performance_schema " + " --outdir sqoop_mysql/src " + " --bindir sqoop_mysql/class"

   os.system(sqp_stmt)


myprops = dict(line.strip().split('=') for line in open('/root/sqoop_workspace/tables.properties'))
for key in myprops:
    values = tuple(myprops[key].split('|'))
    column_list = values[0]
    map_reduce_number = values[1]
    load_table(key, column_list, map_reduce_number)
