package movies

import movies.io.{Reader, Writer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito.{verify, times, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

object MoviesJobSpec extends MockitoSugar {

  class Context {

    implicit val mockReader = mock[Reader]
    implicit val mockWriter = mock[Writer]
  }

  val moviesFileLocation = "movies.dat"
  val ratingsFileLocation = "ratings.dat"
  val outputFolderLocation = "/output"

  case class RatingsInput(movieId: String, averageRating: Double, numberOfVotes: Long)
  case class RatingsOutput(movieId: String, rankingValue: Double, rank: Long)

  object TestData {

    val ratingsData = Seq[RatingsInput](
      RatingsInput("movie-id-1", 5.0d, 4),
      RatingsInput("movie-id-2", 2.1d, 13),
      RatingsInput("movie-id-3", 9.2d, 2))
  }

}

class MoviesJobSpec extends FlatSpec with Matchers with MockitoSugar {

  import MoviesJobSpec._

  "Job.runJob()" should "read the input and write the output" in new Context {
    val ratingsData = Seq[RatingsInput](
      RatingsInput("movie-id-1", 5.0d, 100),
      RatingsInput("movie-id-2", 6.0d, 50),
      RatingsInput("movie-id-3", 4.0d, 30))

    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()

    val ratingsDF = session.createDataFrame[RatingsInput](ratingsData)

    when(mockReader.readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(MoviesJob.ratingsSchema))(mockEq(session))).thenReturn(Success(ratingsDF))
    when(mockWriter.writeDataFrame(any[DataFrame])).thenReturn(Success())

    val output = MoviesJob.runJob(ratingsFileLocation)

    output shouldBe a[Success[_]]
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(MoviesJob.ratingsSchema))(mockEq(session))
    verify(mockWriter, times(1)).writeDataFrame(any[DataFrame])
  }

  "Job.deriveTop20MoviesOnRatingsRanking()" should
    """correctly rank the movies in the input data frame according to the defined ranking function""".stripMargin in new Context {

    val ratingsData = Seq[RatingsInput](
      RatingsInput("movie-id-1", 5.0d, 100),
      RatingsInput("movie-id-2", 6.0d, 50),
      RatingsInput("movie-id-3", 4.0d, 30))

    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()

    val ratingsDF = session.createDataFrame[RatingsInput](ratingsData)
    val expected = Array(
      RatingsOutput("movie-id-1", 8.333333333333334, 1),
      RatingsOutput("movie-id-2", 5.0d, 2)
    )
    val broadcastAverageNumberOfVotes = session.sparkContext.broadcast(60.0d)

    val output = MoviesJob.deriveTop20MoviesOnRatingsRanking(ratingsDF, broadcastAverageNumberOfVotes)
    import session.implicits._

    output.as[RatingsOutput].collect() shouldEqual expected
  }
}
