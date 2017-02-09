Problem Statemnet: Group by key

val products = sc.textFile("/user/cloudera/a_import/products")
val productsMap = products.map(rec => (rec.split(",")(1), rec))
val productsGroupBy = productsMap.groupByKey()
productsGroupBy.collect().foreach(println)