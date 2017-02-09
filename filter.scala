# Filter data into a smaller dataset using Spark

val ordersRDD = sc.textFile("/user/cloudera/a_import/orders")

#equals returns a record with exact same string
ordersRDD.filter(line => line.split(",")(3).equals("COMPLETE")).take(5).foreach(println)

#contains returns any record that contains that string
ordersRDD.filter(line => line.split(",")(3).contains("PENDING")).take(5).foreach(println)

#logical operators can to be used to filter
ordersRDD.filter(line => line.split(",")(0).toInt > 100).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 100 || line.split(",")(3).contains("PENDING")).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 1000 && 
    (line.split(",")(3).contains("PENDING") || line.split(",")(3).equals("CANCELLED"))).
    take(5).
    foreach(println)
    
    
ordersRDD.filter(line => line.split(",")(0).toInt > 1000 && 
    !line.split(",")(3).equals("COMPLETE")).
    take(5).
    foreach(println)