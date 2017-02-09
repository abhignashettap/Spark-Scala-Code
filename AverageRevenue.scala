
#Problem statement: Get the average revenue per day


val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

#parsing order and orderitems
val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

#Join datasets

val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersJoinOrderItemsMap = ordersJoinOrderItems.map(t => ((t._2._2.split(",")(1), t._1), t._2._1.split(",")(4).toFloat))

val revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey((acc, value) => acc + value)

#Parse data to discard order_id and get order_date as key and sum(order_item_subtotal) per order as value
val revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(rec => (rec._1._1, rec._2))

#Use appropriate aggregate function to get sum(order_item_subtotal) per day and count(distinct order_id) per day
val revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey((0.0, 0))(
(acc, revenue) => (acc._1 + revenue, acc._2 + 1), 
(total1, total2) => (total1._1 + total2._1, total1._2 + total2._2) 
)

revenuePerDay.collect().foreach(println)

#Parse data and apply average logic
val avgRevenuePerDay = revenuePerDay.map(x => (x._1, x._2._1/x._2._2))