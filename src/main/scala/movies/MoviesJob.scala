package movies

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object MoviesJob {

  final case class InputDataFrames(ratingsDf: DataFrame, titlesDf: DataFrame, crewsDf: DataFrame,
                                   principalsDf: DataFrame, namesDf: DataFrame)

  def runJob(dataFrames: InputDataFrames)(implicit sparkSession: SparkSession): (DataFrame, DataFrame) = {

    import sparkSession.sqlContext.implicits._
    // persist and repartition as using more than once
    val persistedMovieRatings = filterNonMovies(dataFrames.titlesDf.repartition(200), dataFrames.ratingsDf)
    // assumes that the ratings file is not empty
    val averageNumberOfVotesBroadcast = sparkSession.sparkContext.broadcast(
      persistedMovieRatings.select(avg('numberOfVotes).as("averageNumberOfVotes")).first().getDouble(0))
    // persist as using multiple times going forward
    val top20MoviesByRatings = deriveTop20MoviesOnRatingsRanking(persistedMovieRatings, averageNumberOfVotesBroadcast)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // no longer needed
    persistedMovieRatings.unpersist()

    val principalsJoinedDf = top20MoviesByRatings.join(dataFrames.principalsDf, "titleId")
      .select('principalId.as("personId"))
    val principalsInTop20Df = aggregatePersonCredits(principalsJoinedDf, "principalCredits")
    // as we are exploding this dataframe (creating multiple rows from a single row in the input), it is worth us
    // joining on the top 20 movies before we do the explode function, to reduce the amount of data we shuffle later
    val crewInTop20Df = top20MoviesByRatings.join(dataFrames.crewsDf, "titleId")
      .select('directorIds, 'writerIds)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val directorsInTop20Df = derivePersonCreditsCount(crewInTop20Df, 'directorIds, "directorCredits")
    val writersInTop20Df = derivePersonCreditsCount(crewInTop20Df, 'writerIds, "writerCredits")
    // assume every person id correlates with a name
    val creditsInTop20Movies = principalsInTop20Df
      .join(directorsInTop20Df, Seq("personId"), "outer")
      .join(writersInTop20Df, Seq("personId"), "outer")
      .select(
        'personId,
        (coalesce('principalCredits, lit(0L)) + coalesce('directorCredits, lit(0L)) + coalesce('writerCredits, lit(0L))).as('totalCredits),
        coalesce('directorCredits, lit(0L)).as("directorCredits"),
        coalesce('writerCredits, lit(0L)).as("writerCredits"),
        coalesce('principalCredits, lit(0L)).as("principalCredits"))
      .join(dataFrames.namesDf, "personId")
      .select('primaryName.as("name"), 'totalCredits, 'directorCredits, 'writerCredits, 'principalCredits)
      // keep deterministic for testing, secondary order on name
      .orderBy('totalCredits.desc, 'primaryName)

    (top20MoviesByRatings, creditsInTop20Movies)
  }

  def derivePersonCreditsCount(dataFrame: DataFrame, column: Column, aggregateColumnName: String): DataFrame = {
    import dataFrame.sqlContext.implicits._
    val exploded = dataFrame.select(splitStringUdf(column).as("tmp"))
      .select(explode('tmp).as("personId"))
      aggregatePersonCredits(exploded, aggregateColumnName)
  }

  def aggregatePersonCredits(dataFrame: DataFrame, aggregateColumnName: String): DataFrame = {
    import dataFrame.sqlContext.implicits._
    dataFrame
      .groupBy('personId)
      .agg(count('personId).as(aggregateColumnName))
  }

  def filterNonMovies(titlesDf: DataFrame, ratingsDf: DataFrame): DataFrame = {
    import ratingsDf.sqlContext.implicits._
    titlesDf.filter('titleType === "movie")
      .join(ratingsDf, "titleId")
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
    val window = Window.orderBy('rankingValue.desc, 'numberOfVotes.desc, 'titleId)

    // filter number of votes first to avoid aggregating unnecessary data
    ratingsDf.filter('numberOfVotes >= 50)
      .withColumn("rankingValue",
        (ratingsDf("numberOfVotes") / averageNumberOfVotesBroadcast.value) * ratingsDf("averageRating"))
      // use row number rather than rank/ dense rank
      .withColumn("rank", row_number.over(window))
  }

  // need some custom logic to handle the '\\N'
  val splitStringUdf = udf { string: String => stringToList(string) }

  // could be better
  @inline def stringToList(string: String): List[String] = string match {
    case "" ⇒ Nil
    case "\\N" ⇒ Nil
      // assumes the input string is in the "a,b,c" pattern
    case s ⇒ s.split(",").toList
  }
}