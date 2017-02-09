#Problem Statement: Get the number of orders by order date and order status

val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")
val ordersMapRDD = ordersRDD.map(rec => ((rec.split(",")(1), rec.split(",")(3)), 1))
val ordersByStatusPerDay = ordersMapRDD.reduceByKey((v1, v2) => v1+v2)

ordersByStatusPerDay.collect().foreach(println)


