val data = sc.textFile("/allyelpdata/review.csv")
val filtered_data = data.filter(_.split("::")(20) != "NaN")
val data1 = filtered_data.map(line => line.split("::")).map(line => (line(2), line(20).toInt))
val result =  data1.map { case (key, value) => (key, (value, 1)) }.reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2)}.mapValues {case (value, count) =>  value.toDouble / count.toDouble}
result.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2))
