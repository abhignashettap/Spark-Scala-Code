# Load data using spark

import org.apache.spark.SparkContext

val dataRDD = sc.textFile("/user/cloudera/a_import/customers")
dataRDD.collect().foreach(println)

val customer = dataRDD.count()

#Save as textfile
customer.saveAsTextFile("/user/cloudera/abhi/customers")

#Save as object file
customer.saveAsObjectFile("/user/cloudera/abhi/customersObject")





















