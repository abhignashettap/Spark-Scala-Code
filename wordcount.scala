# Problem Statement: Write a word count program

# Create a file and type few lines and save it as wordcount.txt,copy to HDFS
# to /user/cloudera/wordcount.txt

val data = sc.textFile("/user/cloudera/wordcount.txt")
val dataFlatMap = data.flatMap(x => x.split(" "))
val dataMap = dataFlatMap.map(x => (x, 1))
val dataReduceByKey = dataMap.reduceByKey((x,y) => x + y)

dataReduceByKey.saveAsTextFile("/user/cloudera/wordcountoutput")