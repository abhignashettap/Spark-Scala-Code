#Problem Statement: Check if there are any cancelled orders with amount greater than 1000$


val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/a_import/order_items")

#Get only cancelled orders
val ordersParsedRDD = ordersRDD.filter(rec => rec.split(",")(3).contains("CANCELED")).
  map(rec => (rec.split(",")(0).toInt, rec))
val orderItemsParsedRDD = orderItemsRDD.
  map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
  
#Generate sum(order_item_subtotal) per order
val orderItemsAgg = orderItemsParsedRDD.reduceByKey((acc, value) => (acc + value))

#Join orders and order items
val ordersJoinOrderItems = orderItemsAgg.join(ordersParsedRDD)

#Filter data which amount to greater than 1000$
ordersJoinOrderItems.filter(rec => rec._2._1 >= 1000).take(5).foreach(println)