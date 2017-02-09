
# Problem Statement: Find the Customer id with max revenue

val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/a_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

# Join the datasets
val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersPerDayPerCustomer = ordersJoinOrderItems.map(rec => ((rec._2._2.split(",")(1), rec._2._2.split(",")(2)), rec._2._1.split(",")(4).toFloat))
val revenuePerDayPerCustomer = ordersPerDayPerCustomer.reduceByKey((x, y) => x + y)

val revenuePerDayPerCustomerMap = revenuePerDayPerCustomer.map(rec => (rec._1._1, (rec._1._2, rec._2)))
val topCustomerPerDaybyRevenue = revenuePerDayPerCustomerMap.reduceByKey((x, y) => (if(x._2 >= y._2) x else y))

#Using regular function
def findMax(x: (String, Float), y: (String, Float)): (String, Float) = {
  if(x._2 >= y._2)
    return x
  else
    return y
}

val topCustomerPerDaybyRevenue = revenuePerDayPerCustomerMap.reduceByKey((x, y) => findMax(x, y))