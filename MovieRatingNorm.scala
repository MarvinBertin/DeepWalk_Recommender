val sampleFile  = "file:///netflix_data.csv"

val ratings = sc.textFile(new File(sampleFile).toString)
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map { line =>
  val fields = line.split(",")
  // format: (Index,UID, Rating, MID)
  // Rating(userId, MID, Rating)
  Array(fields(1).toInt, fields(3).toInt,fields(2).toInt).toNDArray


val users = ratings.map(s => s(0)).distinct.zipWithIndex.toArray.toMap
val movies = ratings.map(s => s(1)).distinct.zipWithIndex.toArray.toMap

val numRatings = ratings.count.toInt
val numUsers = ratings.map(s => s(0)).distinct.count.toInt
val numMovies = ratings.map(s => s(1)).distinct.count.toInt

val ratingsarr = ratings.map(s => s(2)).toArray.toNDArray
val moviesids = ratings.map(s => s(1)).toArray
val userids = ratings.map(s => s(0)).toArray

val moviemaparr = moviesids.map(s => movies.getOrElse(s,0))
val usermaparr = userids.map(s => users.getOrElse(s,0))

val meanrating = org.nd4j.linalg.factory.Nd4j.mean(ratingsarr)(0)
val stdrating = org.nd4j.linalg.factory.Nd4j.std(ratingsarr)(0)

val adjratingarr = (ratingsarr - meanrating) / stdrating

val arr = Array(0).toNDArray
val V = arr.reshape(numUsers,numMovies)

for(i <- 0 until  numRatings){
    val row = usermaparr(i).asInstanceOf[Number].intValue()
    val column = moviemaparr(i).asInstanceOf[Number].intValue()
    V(row,column) = adjratingarr(i)
}
// UserUser
val UU = V dot V.transpose

// ItemItem
val MM = V.transpose dot V