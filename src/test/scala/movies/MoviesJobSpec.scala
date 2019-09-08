package movies

import movies.Driver.InputFilePaths
import movies.MoviesJob.InputDataFrames
import movies.io.{Reader, Writer}
import org.apache.spark.sql.SparkSession
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

object MoviesJobSpec extends MockitoSugar {

  class Context {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    implicit val mockReader = mock[Reader]
    implicit val mockWriter = mock[Writer]
    val ratingsDf       = session.createDataFrame(TestData.ratingsData)
    val titlesDf        = session.createDataFrame(TestData.titlesData)
    val crewsDf         = session.createDataFrame(TestData.crewsData)
    val principalsDf    = session.createDataFrame(TestData.principalsData)
    val namesDf         = session.createDataFrame(TestData.namesData)
    val inputDataFrames = InputDataFrames(ratingsDf, titlesDf, crewsDf, principalsDf, namesDf)
  }

  val ratingsFileLocation    = "ratings.tsv"
  val titlesFileLocation     = "titles.tsv"
  val crewsFileLocation      = "crews.tsv"
  val principalsFileLocation = "principals.tsv"
  val namesFileLocation      = "names.tsv"
  val inputFilePaths         = InputFilePaths(ratingsFileLocation, titlesFileLocation, crewsFileLocation,
    principalsFileLocation, namesFileLocation)

  final case class RatingsInput(titleId: String, averageRating: Double, numberOfVotes: Long)
  final case class TitlesInput(titleId: String, titleType: String, primaryTitle: String, originalTitle: String,
                               isAdult: String, startYear: String, endYear: String, runtimeMinutes: String,
                               genres: String)
  final case class CrewsInput(titleId: String, directorIds: String, writerIds: String)
  final case class PrincipalsInput(titleId: String, ordering: Integer, principalId: String, category: String,
                                   job: String, characters: String)
  final case class NamesInput(personId: String, primaryName: String, birthYear: String, deathYear: String,
                              primaryProfession: String, knownForTitles: String)

  final case class Top20Output(titleId: String, rankingValue: Double, rank: Long)
  final case class CreditsOutput(name: String, totalCredits: Long, directorCredits: Long, writerCredits: Long,
                                 principalCredits: Long)

  object TestData {

    val ratingsData = Seq[RatingsInput](
      RatingsInput("movie-id-1", 5.0d, 60),
      RatingsInput("movie-id-2", 2.1d, 110),
      RatingsInput("movie-id-3", 9.2d, 30),
      RatingsInput("movie-id-4", 4.6d, 65),
      RatingsInput("movie-id-5", 9.6d, 65))

    val titlesData = Seq[TitlesInput](
      TitlesInput("movie-id-1", "movie", "some-title", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-2", "movie", "some-title2", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-3", "short", "some-title3", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-4", "movie", "some-title4", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-5", "movie", "some-title5", "a", "b", "c", "d", "e", "f")
    )

    val crewsData = Seq[CrewsInput](
      CrewsInput("movie-id-1", "director1,director2,director3", "writer1"),
      // writer 1 also has a director credit
      CrewsInput("movie-id-2", "director2,director3,writer1", "\\N"),
      CrewsInput("movie-id-3", "director1,director2,director3", "writer3"),
      CrewsInput("movie-id-4", "\\N", "writer1,writer2"),
      CrewsInput("movie-id-5", "\\N", "writer2")
    )

    val principalsData = Seq[PrincipalsInput](
      PrincipalsInput("movie-id-1", 1, "principal1", "a", "b", "c"),
      PrincipalsInput("movie-id-1", 2, "principal2", "a", "b", "c"),
      PrincipalsInput("movie-id-1", 3, "principal3", "a", "b", "c"),
      PrincipalsInput("movie-id-1", 4, "principal4", "a", "b", "c"),
      PrincipalsInput("movie-id-2", 1, "principal2", "a", "b", "c"),
      PrincipalsInput("movie-id-2", 2, "principal3", "a", "b", "c"),
      PrincipalsInput("movie-id-3", 1, "principal4", "a", "b", "c"),
      PrincipalsInput("movie-id-5", 1, "principal1", "a", "b", "c"),
      PrincipalsInput("movie-id-5", 2, "principal2", "a", "b", "c"),
      PrincipalsInput("movie-id-5", 2, "writer1", "a", "b", "c")
    )

    val namesData = Seq[NamesInput](
      NamesInput("principal1", "first principal", "", "", "", ""),
      NamesInput("principal2", "second principal", "", "", "", ""),
      NamesInput("principal3", "third principal", "", "", "", ""),
      NamesInput("principal4", "fourth principal", "", "", "", ""),
      NamesInput("writer1", "first writer", "", "", "", ""),
      NamesInput("writer2", "second writer", "", "", "", ""),
      NamesInput("writer3", "third writer", "", "", "", ""),
      NamesInput("director1", "first director", "", "", "", ""),
      NamesInput("director2", "second director", "", "", "", ""),
      NamesInput("director3", "third director", "", "", "", "")
    )
  }
}

class MoviesJobSpec extends FlatSpec with Matchers with MockitoSugar {

  import MoviesJobSpec._

  "Job.runJob()" should "read the input and write the output" in new Context {

    // different result from other test due to the non-movie item being filtered
    val expectedTop20 = Array(
      Top20Output("movie-id-5", 8.32d, 1),
      Top20Output("movie-id-1", 4.0d, 2),
      Top20Output("movie-id-4", 3.9866666666666664d, 3),
      Top20Output("movie-id-2", 3.08d, 4)
    )

    val expectedCredits = Array(
      CreditsOutput("first writer", 4, 1, 2, 1),
      CreditsOutput("second principal", 3, 0, 0, 3),
      CreditsOutput("first principal", 2, 0, 0, 2),
      CreditsOutput("second director", 2, 2, 0, 0),
      CreditsOutput("second writer", 2, 0, 2, 0),
      CreditsOutput("third director", 2, 2, 0, 0),
      CreditsOutput("third principal", 2, 0, 0, 2),
      CreditsOutput("first director", 1, 1, 0, 0),
      CreditsOutput("fourth principal", 1, 0, 0, 1)
    )

    val output = MoviesJob.runJob(inputDataFrames)

    import session.implicits._

    output._1.as[Top20Output].collect() shouldEqual expectedTop20
    output._2.as[CreditsOutput].collect() shouldEqual expectedCredits
  }

  "Job.deriveTop20MoviesOnRatingsRanking()" should
    "correctly rank the movies in the input data frame according to the defined ranking function" in new Context {

    val expected = Array(
      Top20Output("movie-id-5", 10.399999999999999d, 1),
      Top20Output("movie-id-1", 5.0d, 2),
      Top20Output("movie-id-4", 4.9833333333333325d, 3),
      Top20Output("movie-id-2", 3.85d, 4)
    )
    val broadcastAverageNumberOfVotes = session.sparkContext.broadcast(60.0d)

    val output = MoviesJob.deriveTop20MoviesOnRatingsRanking(ratingsDf, broadcastAverageNumberOfVotes)
    import session.implicits._

    output.as[Top20Output].collect() shouldEqual expected
  }
}
