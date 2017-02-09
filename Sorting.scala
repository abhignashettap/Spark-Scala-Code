# Write a query that produces ranked or sorted data using Spark

val orders = sc.textFile("/user/cloudera/sqoop_import/orders")

#Global sorting -ascending
orders.map(rec => (rec.split(",")(0).toInt, rec)).sortByKey().collect().foreach(println)

#Global sorting -descending
orders.map(rec => (rec.split(",")(0).toInt, rec)).sortByKey(false).take(5).foreach(println)

#Gives top 5 elements
orders.map(rec => (rec.split(",")(0).toInt, rec)).top(5).foreach(println)

orders.map(rec => (rec.split(",")(0).toInt, rec)).takeOrdered(5).foreach(println)

orders.map(rec => (rec.split(",")(0).toInt, rec)).takeOrdered(5)(Ordering[Int]
                                      .reverse.on(x => x._1)).foreach(println)

orders.takeOrdered(5)(Ordering[Int].on(x => x.split(",")(0).toInt)).foreach(println)
orders.takeOrdered(5)(Ordering[Int].reverse.on(x => x.split(",")(0).toInt)).foreach(println)


