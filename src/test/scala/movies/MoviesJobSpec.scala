package movies

import movies.Driver.InputFilePaths
import movies.io.{Reader, Writer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

object MoviesJobSpec extends MockitoSugar {

  class Context {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    implicit val mockReader = mock[Reader]
    implicit val mockWriter = mock[Writer]
    val ratingsDf    = session.createDataFrame(TestData.ratingsData)
    val titlesDf     = session.createDataFrame(TestData.titlesData)
    val crewsDf      = session.createDataFrame(TestData.crewsData)
    val principalsDf = session.createDataFrame(TestData.principalsData)
    val namesDf      = session.createDataFrame(TestData.namesData)
  }

  val ratingsFileLocation    = "ratings.tsv"
  val titlesFileLocation     = "titles.tsv"
  val crewsFileLocation      = "crews.tsv"
  val principalsFileLocation = "principals.tsv"
  val namesFileLocation      = "names.tsv"
  val inputFilePaths         = InputFilePaths(ratingsFileLocation, titlesFileLocation, crewsFileLocation,
    principalsFileLocation, namesFileLocation)

  final case class RatingsInput(titleId: String, averageRating: Double, numberOfVotes: Long)
  final case class RatingsOutput(titleId: String, rankingValue: Double, rank: Long)
  final case class TitlesInput(titleId: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: String,
                         startYear: String, endYear: String, runtimeMinutes: String, genres: String)
  final case class CrewsInput(titleId: String, directorIds: String, writerIds: String)
  final case class PrincipalsInput(titleId: String, ordering: Integer, principalId: String, category: String, job: String, characters: String)
  final case class NamesInput(personId: String, primaryName: String, birthYear: String, deathYear: String, primaryProfession: String, knownForTitles: String)

  object TestData {

    val ratingsData = Seq[RatingsInput](
      RatingsInput("movie-id-1", 5.0d, 54),
      RatingsInput("movie-id-2", 2.1d, 123),
      RatingsInput("movie-id-3", 9.2d, 1),
      RatingsInput("movie-id-4", 4.6d, 110),
      RatingsInput("movie-id-5", 4.6d, 140))

    val titlesData = Seq[TitlesInput](
      TitlesInput("movie-id-1", "movie", "some-title", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-2", "movie", "some-title2", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-3", "short", "some-title3", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-4", "movie", "some-title4", "a", "b", "c", "d", "e", "f"),
      TitlesInput("movie-id-5", "short", "some-title5", "a", "b", "c", "d", "e", "f")
    )

    val crewsData = Seq[CrewsInput](
      CrewsInput("movie-id-1", "director1,director2,director3", "writer1"),
      CrewsInput("movie-id-2", "director2,director3", "\\N"),
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
      PrincipalsInput("movie-id-5", 2, "principal2", "a", "b", "c")
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
  import MoviesJobSpec.TestData._

  "Job.runJob()" should "read the input and write the output" in new Context {

    when(mockReader.readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(Schemas.ratingsSchema))(mockEq(session))).thenReturn(Success(ratingsDf))
    when(mockReader.readFileToDataFrame(mockEq(titlesFileLocation), mockEq(Schemas.titlesSchema))(mockEq(session))).thenReturn(Success(titlesDf))
    when(mockReader.readFileToDataFrame(mockEq(crewsFileLocation), mockEq(Schemas.crewSchema))(mockEq(session))).thenReturn(Success(crewsDf))
    when(mockReader.readFileToDataFrame(mockEq(principalsFileLocation), mockEq(Schemas.principalsSchema))(mockEq(session))).thenReturn(Success(principalsDf))
    when(mockReader.readFileToDataFrame(mockEq(namesFileLocation), mockEq(Schemas.namesSchema))(mockEq(session))).thenReturn(Success(namesDf))
    when(mockWriter.writeDataFrame(any[DataFrame])).thenReturn(Success())

    val output = MoviesJob.runJob(inputFilePaths)

    output shouldBe a[Success[_]]
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(Schemas.ratingsSchema))(mockEq(session))
    verify(mockWriter, times(2)).writeDataFrame(any[DataFrame])
  }

  "Job.deriveTop20MoviesOnRatingsRanking()" should
    """correctly rank the movies in the input data frame according to the defined ranking function""".stripMargin in new Context {

    val expected = Array(
      RatingsOutput("movie-id-5", 10.733333333333333d, 1),
      RatingsOutput("movie-id-4", 8.433333333333332d, 2),
      RatingsOutput("movie-id-1", 4.5d, 3),
      RatingsOutput("movie-id-2", 4.305d, 4)
    )
    val broadcastAverageNumberOfVotes = session.sparkContext.broadcast(60.0d)

    val output = MoviesJob.deriveTop20MoviesOnRatingsRanking(ratingsDf, broadcastAverageNumberOfVotes)
    import session.implicits._

    output.as[RatingsOutput].collect() shouldEqual expected
  }
}
