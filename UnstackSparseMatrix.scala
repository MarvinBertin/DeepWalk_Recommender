
object Cells {
  // Line format: (0,{0:5.3,1:4.5,4:3.4})
  val text = sc.textFile("data/ItemItemMatrixSample.txt")

  /* ... new cell ... */

  text.take(3)

  /* ... new cell ... */

  def trimString(string: String): String = string.substring(1, string.length - 1)

  /* ... new cell ... */

  def trimFront(string: String): String = string.substring(1, string.length)

  /* ... new cell ... */

  def parser(idx: Int, dict: String): Array[List[String]] = {
      trimString(dict)
      .split(",")
      .filter(x => x(0).toString.toInt < idx)
      .map { x =>
            val edgeScore = x.split(":")
            List(idx.toString, edgeScore(0), edgeScore(1))}
  }

  /* ... new cell ... */

  text.map { x => 
            val xTrim = trimString(x)
            (xTrim.head, trimFront(xTrim.tail))
           }.flatMap(x => parser(x._1.toString.toInt, x._2))
            .map(_.mkString(","))
            .take(10)
            //.coalesce(4)
            //.saveAsTextFile("data/ItemItemEdges")
}
              