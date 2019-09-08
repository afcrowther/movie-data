package movies

import movies.Driver.InputFilePaths
import movies.DriverSpec.Output
import movies.MoviesJob.InputDataFrames
import movies.MoviesJobSpec.{Context, TestData, crewsFileLocation, inputFilePaths, mock, namesFileLocation, principalsFileLocation, ratingsFileLocation, titlesFileLocation}
import movies.io.{Reader, Writer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

object DriverSpec {

  class Context {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    implicit val mockReader = mock[Reader]
    implicit val mockWriter = mock[Writer]
    val ratingsDf    = session.createDataFrame(TestData.ratingsData)
    val titlesDf     = session.createDataFrame(TestData.titlesData)
    val crewsDf      = session.createDataFrame(TestData.crewsData)
    val principalsDf = session.createDataFrame(TestData.principalsData)
    val namesDf      = session.createDataFrame(TestData.namesData)
    val inputDataFrames = InputDataFrames(ratingsDf, titlesDf, crewsDf, principalsDf, namesDf)
  }

  val ratingsFileLocation    = "ratings.tsv"
  val titlesFileLocation     = "titles.tsv"
  val crewsFileLocation      = "crews.tsv"
  val principalsFileLocation = "principals.tsv"
  val namesFileLocation      = "names.tsv"
  val inputFilePaths         = InputFilePaths(ratingsFileLocation, titlesFileLocation, crewsFileLocation,
    principalsFileLocation, namesFileLocation)

  final case class Output(someData: String, other: Int)
}

class DriverSpec extends FlatSpec with Matchers with MockitoSugar {

  "Driver.initializeJob()" should "complete successfully" in new Context {
    when(mockReader.readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(Schemas.ratingsSchema))(mockEq(session))).thenReturn(Success(ratingsDf))
    when(mockReader.readFileToDataFrame(mockEq(titlesFileLocation), mockEq(Schemas.titlesSchema))(mockEq(session))).thenReturn(Success(titlesDf))
    when(mockReader.readFileToDataFrame(mockEq(crewsFileLocation), mockEq(Schemas.crewSchema))(mockEq(session))).thenReturn(Success(crewsDf))
    when(mockReader.readFileToDataFrame(mockEq(principalsFileLocation), mockEq(Schemas.principalsSchema))(mockEq(session))).thenReturn(Success(principalsDf))
    when(mockReader.readFileToDataFrame(mockEq(namesFileLocation), mockEq(Schemas.namesSchema))(mockEq(session))).thenReturn(Success(namesDf))
    when(mockWriter.writeDataFrame(any[DataFrame])).thenReturn(Success())

    val output = Driver.initializeJob(
      ip => session => (session.createDataFrame[Output](Seq(Output("1", 1))), session.createDataFrame[Output](Seq(Output("2", 2)))),
      inputFilePaths, mockReader, mockWriter, true)

    output shouldBe a[Success[_]]
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(ratingsFileLocation), mockEq(Schemas.ratingsSchema))(mockEq(session))
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(titlesFileLocation), mockEq(Schemas.titlesSchema))(mockEq(session))
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(crewsFileLocation), mockEq(Schemas.crewSchema))(mockEq(session))
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(principalsFileLocation), mockEq(Schemas.principalsSchema))(mockEq(session))
    verify(mockReader, times(1)).readFileToDataFrame(mockEq(namesFileLocation), mockEq(Schemas.namesSchema))(mockEq(session))
    verify(mockWriter, times(2)).writeDataFrame(any[DataFrame])
  }
}
