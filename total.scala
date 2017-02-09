#Problem statement: Get the Total Revenue per day

val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/a_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersJoinOrderItemsMap = ordersJoinOrderItems.map(t => (t._2._2.split(",")(1), t._2._1.split(",")(4).toFloat))

#apply tranformation - reduceByKey to get the sum
val revenuePerDay = ordersJoinOrderItemsMap.reduceByKey((acc, value) => acc + value)
revenuePerDay.collect().foreach(println)