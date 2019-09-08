package movies

import movies.io.{Reader, Writer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object MoviesJob {

  final case class InputDataFrames(ratingsDf: DataFrame)

  val ratingsSchema = StructType(Array(
    StructField("movieId", StringType),
    StructField("averageRating", DoubleType),
    StructField("numberOfVotes", LongType)
  ))

  def runJob(ratingsFileLocation: String)(implicit sparkSession: SparkSession, reader: Reader, writer: Writer): Try[Unit] = {

    import sparkSession.sqlContext.implicits._

    val inputDataFrames = for {
      ratingsDf ← reader.readFileToDataFrame(ratingsFileLocation, ratingsSchema)
    } yield InputDataFrames(ratingsDf)

    inputDataFrames.map { dataFrames ⇒
      // persist and repartition as using more than once
      val persistedRatings = dataFrames.ratingsDf.repartition(200).persist(StorageLevel.MEMORY_AND_DISK_2)
      // assumes that the ratings file is not empty
      val averageNumberOfVotesBroadcast = sparkSession.sparkContext.broadcast(
        persistedRatings.select(avg('numberOfVotes).as("averageNumberOfVotes")).first().getDouble(0))
      val top20MoviesByRatings = deriveTop20MoviesOnRatingsRanking(persistedRatings, averageNumberOfVotesBroadcast)
      writer.writeDataFrame(top20MoviesByRatings)
    }

  }

  /**
   * Will return the top twenty movies based on the ranking function:
   * ranking value = (number of votes / average number of votes) * average rating
   */
  def deriveTop20MoviesOnRatingsRanking(ratingsDf: DataFrame, averageNumberOfVotesBroadcast: Broadcast[Double])
                                       (implicit session: SparkSession): DataFrame = {

    import ratingsDf.sqlContext.implicits._

    // add tie-breaker of numberOfVotes and movieId on ordering so that we remain deterministic (movie id assumed to be unique)
    val window = Window.orderBy('rankingValue.desc, 'numberOfVotes.desc, 'movieId)

    // filter number of votes first to avoid aggregating unnecessary data
    ratingsDf.filter('numberOfVotes >= 50)
      .withColumn("rankingValue",
        (ratingsDf("numberOfVotes") / averageNumberOfVotesBroadcast.value) * ratingsDf("averageRating"))
      // use row number rather than rank/ dense rank
      .withColumn("rank", row_number.over(window))
  }
}