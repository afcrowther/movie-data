package movies.io

import java.io.FileNotFoundException

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Try}

class Reader {

  def readFileToDataFrame(filePath: String, schema: StructType)(implicit session: SparkSession): Try[DataFrame] = {

    Try {
      session.read
        .option("sep", "\t")
        .option("header", true)
        .schema(schema)
        .csv(filePath)
    }.recoverWith {
      // for now just assume any error is an invalid file path
      case th: Throwable => Failure(new FileNotFoundException(s"Invalid input file location: $filePath"))
    }
  }
}
