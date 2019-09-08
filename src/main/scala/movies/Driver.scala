package movies

import movies.io.{ConsoleWriter, Reader, Writer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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
      ifp => (session, reader, writer) => MoviesJob.runJob(ifp)(session, reader, writer),
      inputFilePaths) match {
      case Success(_) => log.info("Job successful")
      case Failure(th) => log.error(s"Job failed with exception message: ${th.getMessage}")
    }
  }

  private[movies] def initializeJob(job: InputFilePaths => (SparkSession, Reader, Writer) => Try[Unit],
                                    inputFilePaths: InputFilePaths,
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
    }.flatMap { session =>
      val reader = new Reader()
      val writer = new ConsoleWriter()
      val out = job(inputFilePaths)(session, reader, writer)
      session.stop()
      out
    }
  }
}
