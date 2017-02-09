#save as SequenceFile without key(key as null)
import org.apache.hadoop.io._
dataRDD.map(x => (NullWritable.get(), x)).saveAsSequenceFile("/user/cloudera/abhi/customersSeq")

#save as SequenceFile with key
dataRDD.map(x => (x.split(",")(0), x.split(",")(1))).saveAsSequenceFile("/user/cloudera/scalaspark/departmentsSeq")

#save as SequenceFile with key in New API Hadoop format
import org.apache.hadoop.mapreduce.lib.output._                #import statement required

val path="/user/cloudera/scalaspark/departmentsSeq"
dataRDD.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1))))
 .saveAsNewAPIHadoopFile(path, classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])

#read a sequence file
sc.sequenceFile("/user/cloudera/spark/departmentsSeq", classOf[IntWritable], classOf[Text])
     .map(rec => rec.toString()).collect().foreach(println)