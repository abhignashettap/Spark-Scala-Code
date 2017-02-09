# Join disparate datasets together using Spark

# Problem statement: Get the revenue and number of orders from order_items on daily basis

val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/a_import/order_items")

#apply transformations
val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec))
 
 #Join datasets
val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val revenuePerOrderPerDay = ordersJoinOrderItems.map(t => (t._2._2.split(",")(1), t._2._1.split(",")(4).toFloat))

# Get order count per day
val ordersPerDay = ordersJoinOrderItems.map(rec => rec._2._2.split(",")(1) + "," + rec._1).distinct()
val ordersPerDayParsedRDD = ordersPerDay.map(rec => (rec.split(",")(0), 1))
val totalOrdersPerDay = ordersPerDayParsedRDD.reduceByKey((x, y) => x + y)

# Get revenue per day from joined data
val totalRevenuePerDay = revenuePerOrderPerDay.reduceByKey(
  (total1, total2) => total1 + total2 
)

totalRevenuePerDay.sortByKey().collect().foreach(println)
