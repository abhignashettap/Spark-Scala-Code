#Get data sorted by product price per category


#Map will return the list - ascending
productsGroupBy.map(rec => (rec._2.toList.sortBy(k => k.split(",")(4).toFloat))).
  take(100).
  foreach(println)
  
  
#Map will return the list - descending
productsGroupBy.map(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)
  
#flatMap will return one record per line
productsGroupBy.flatMap(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

