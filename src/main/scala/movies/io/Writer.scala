package movies.io

import org.apache.spark.sql.DataFrame

import scala.util.Try

trait Writer {

  def writeDataFrame(dataFrame: DataFrame): Try[Unit]
}
