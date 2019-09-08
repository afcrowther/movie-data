package movies

import movies.MoviesJob.InputDataFrames
import movies.io.{ConsoleWriter, Reader, Writer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object Driver {

  val log = LoggerFactory.getLogger(Driver.getClass)

  final case class InputFilePaths(ratingsFilePath: String, titlesFilePath: String, crewsFilePath: String,
                                  principalFilePath: String, namesFilePath: String)

  def main(args: Array[String]): Unit = {

    require(args.length == 5,
      s"""Expected all file paths to be provided:
         |ratingsFilePath: String,
         |titlesFilePath: String,
         |crewsFilePath: String,
         |principalFilePath: String,
         |namesFilePath: String
         |""".stripMargin)

    val inputFilePaths = InputFilePaths(args(0), args(1), args(2), args(3), args(4))
    initializeJob(
      idf => (session) => MoviesJob.runJob(idf)(session),
      inputFilePaths, new Reader(), new ConsoleWriter()) match {
      case Success(()) => log.info("Job successful")
      case Failure(th) => log.error(s"Job failed with exception message: ${th.getMessage}")
    }
  }

  private[movies] def initializeJob(job: InputDataFrames => SparkSession => (DataFrame, DataFrame),
                                    inputFilePaths: InputFilePaths,
                                    reader: Reader,
                                    writer: Writer,
                                    isTest: Boolean = false): Try[Unit] = {

    Try {
      val sparkConf = new SparkConf()
        .setAppName("movies")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired", "true")
        .registerKryoClasses(Array(
          classOf[Reader],
          classOf[ConsoleWriter]))
      SparkSession.builder()
        .config(if (isTest) sparkConf.setMaster("local[*]") else sparkConf)
        .getOrCreate()
    }.flatMap { implicit session =>
      val out = for {
        ratingsDf       ← reader.readFileToDataFrame(inputFilePaths.ratingsFilePath, Schemas.ratingsSchema)
        titlesDf        ← reader.readFileToDataFrame(inputFilePaths.titlesFilePath, Schemas.titlesSchema)
        crewsDf         ← reader.readFileToDataFrame(inputFilePaths.crewsFilePath, Schemas.crewSchema)
        principalsDf    ← reader.readFileToDataFrame(inputFilePaths.principalFilePath, Schemas.principalsSchema)
        namesDf         ← reader.readFileToDataFrame(inputFilePaths.namesFilePath, Schemas.namesSchema)
        inputDataFrames = InputDataFrames(ratingsDf, titlesDf, crewsDf, principalsDf, namesDf)
        out             = job(inputDataFrames)(session)
        top20Write      ← writer.writeDataFrame(out._1)
        creditsWrite    ← writer.writeDataFrame(out._2)
      } yield creditsWrite
      session.stop()
      out
    }
  }
}
