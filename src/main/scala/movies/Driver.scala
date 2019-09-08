package movies

import movies.io.{ConsoleWriter, Reader, Writer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object Driver {

  val log = LoggerFactory.getLogger(Driver.getClass)

  def main(args: Array[String]): Unit = {

    require(args.length == 1, "Expected input ratings file path")

    initializeJob(
      ratingsFile => (session, reader, writer) => MoviesJob.runJob(ratingsFile)(session, reader, writer),
      args(0),
      args(1),
      args(2).stripSuffix("/")) match {
      case Success(_) => log.info("Job successful")
      case Failure(th) => log.error(s"Job failed with exception message: ${th.getMessage}")
    }
  }

  private[movies] def initializeJob(job: (String) => (SparkSession, Reader, Writer) => Try[Unit],
    inputMovieFilePath: String,
    inputRatingsFilePath: String,
    outputFolderLocation: String,
    isTest: Boolean = false): Try[Unit] = {

    Try {
      val sparkConf = new SparkConf()
        .setAppName("movies")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired", "true")
        .registerKryoClasses(Array(
          classOf[Reader]))
      SparkSession.builder()
        .config(if (isTest) sparkConf.setMaster("local[*]") else sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }.flatMap { session =>
      val reader = new Reader()
      val writer = new ConsoleWriter()
      val out = job(inputRatingsFilePath)(session, reader, writer)
      session.stop()
      out
    }
  }
}
