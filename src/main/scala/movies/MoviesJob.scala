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

  final case class InputDataFrames(ratingsDf: DataFrame, titlesDf: DataFrame, crewsDf: DataFrame,
                                   principalsDf: DataFrame, namesDf: DataFrame)

  def runJob(ratingsFileLocation: String, titlesFileLocation: String, crewsFileLocation: String,
             principalsFileLocation: String, namesFileLocation: String)
            (implicit sparkSession: SparkSession, reader: Reader, writer: Writer): Try[Unit] = {

    import sparkSession.sqlContext.implicits._

    val inputDataFrames = for {
      ratingsDf    ← reader.readFileToDataFrame(ratingsFileLocation, Schemas.ratingsSchema)
      titlesDf     ← reader.readFileToDataFrame(titlesFileLocation, Schemas.titlesSchema)
      crewsDf      ← reader.readFileToDataFrame(crewsFileLocation, Schemas.crewSchema)
      principalsDf ← reader.readFileToDataFrame(principalsFileLocation, Schemas.principalsSchema)
      namesDf      ← reader.readFileToDataFrame(namesFileLocation, Schemas.namesSchema)
    } yield InputDataFrames(ratingsDf, titlesDf, crewsDf, principalsDf, namesDf)

    inputDataFrames.flatMap { dataFrames ⇒
      // persist and repartition as using more than once
      val persistedMovieRatings = filterNonMovies(dataFrames.titlesDf.repartition(200), dataFrames.ratingsDf)
      // assumes that the ratings file is not empty
      val averageNumberOfVotesBroadcast = sparkSession.sparkContext.broadcast(
        persistedMovieRatings.select(avg('numberOfVotes).as("averageNumberOfVotes")).first().getDouble(0))
      // persist as using multiple times going forward
      val top20MoviesByRatings = deriveTop20MoviesOnRatingsRanking(persistedMovieRatings, averageNumberOfVotesBroadcast)
        .persist(StorageLevel.MEMORY_AND_DISK)
      // write top 20 movies
      writer.writeDataFrame(top20MoviesByRatings)

      // no longer needed
      persistedMovieRatings.unpersist()
      val principalsInTop20Df = top20MoviesByRatings.join(dataFrames.principalsDf, 'movieId)
        .select('principalId.as("personId"))
        .groupBy('personId)
        .agg(count('personId).as("credits"))
      // as we are exploding this dataframe (creating multiple rows from a single row in the input), it is worth us
      // joining on the top 20 movies before we do the explode function, to reduce the amount of data we shuffle later
      val crewInTop20Df = top20MoviesByRatings.join(dataFrames.crewsDf, 'movieId)
        .select('directors, 'writers)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val directorsInTop20Df = crewInTop20Df.select(splitStringUdf('directors).as("directors"))
        .select(explode('directors).as("personId"))
        .groupBy('personId)
        .agg(count('personId).as("credits"))
      val writersInTop20Df = crewInTop20Df.select('movieId, splitStringUdf('writers).as("writers"))
        .select(explode('writers).as("personId"))
        .groupBy('personId)
        .agg(count('personId).as("credits"))

      // assume every person id correlates with a name
      val creditsInTop20Movies = principalsInTop20Df
        .join(directorsInTop20Df, 'personId)
        .join(writersInTop20Df, 'personId)
        .groupBy('personId)
        .agg(sum('credits))
        .join(dataFrames.namesDf, 'personId)
        .select('primaryName.as("name"), 'credits.as("credits_in_top_20_films"))
      writer.writeDataFrame(creditsInTop20Movies)
    }
  }

  def filterNonMovies(titlesDf: DataFrame, ratingsDf: DataFrame): DataFrame = {
    import ratingsDf.sqlContext.implicits._
    titlesDf.filter('titleType === "movie")
      .join(ratingsDf, 'titleId)
      .select('titleId, 'averageRating, 'numberOfVotes, 'primaryTitle)
  }


  /**
   * Will return the top twenty movies based on the ranking function:
   * ranking value = (number of votes / average number of votes) * average rating
   */
  def deriveTop20MoviesOnRatingsRanking(ratingsDf: DataFrame, averageNumberOfVotesBroadcast: Broadcast[Double])
                                       (implicit session: SparkSession): DataFrame = {

    import ratingsDf.sqlContext.implicits._

    // add tie-breaker of numberOfVotes and movieId on ordering so that we remain deterministic (title id assumed to be unique)
    val window = Window.orderBy('rankingValue.desc, 'numberOfVotes.desc, 'movieId)

    // filter number of votes first to avoid aggregating unnecessary data
    ratingsDf.filter('numberOfVotes >= 50)
      .withColumn("rankingValue",
        (ratingsDf("numberOfVotes") / averageNumberOfVotesBroadcast.value) * ratingsDf("averageRating"))
      // use row number rather than rank/ dense rank
      .withColumn("rank", row_number.over(window))
  }

  // need some custom logic to handle the '\\N
  val splitStringUdf = udf { string: String => stringToList(string) }

  // could be better
  def stringToList(string: String): List[String] = string match {
    case "" ⇒ Nil
    case "\\N" ⇒ Nil
      // assumes the input string is in the "a,b,c" pattern
    case s ⇒ s.split(",").toList
  }
}