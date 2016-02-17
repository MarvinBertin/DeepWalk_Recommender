// symmetric matrix only need to loop over triangle matrix
val ItemCombinations = sc.parallelize(
  for {
    i <- 0 until numMovie
    j <- 0 to i
} yield (i, j))

val ItemEdges = ItemCombinations
	.filter(x => IM.apply(x._1, x._2) != 0) // may be different with sparse Matrix
	.map(x => List(x._1, x._2, IM.apply(x._1, x._2))) //(itemNode_i, itemNode_j, edgeWeight)

// save weighted edges to file
ItemEdges.map(x => x.mkString(","))
	.coalesce(1)
	.saveAsTextFile("data/ItemItemEdges")