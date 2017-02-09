#copying to HDFS (using linux command line)
hadoop fs -put departments.txt /user/cloudera/abhi

#copying to local (using linux command line)
hadoop fs -get departments.txt /user/cloudera/abhi

#validating Data on hdfs
hadoop fs -cat departments.txt /user/cloudera/abhi