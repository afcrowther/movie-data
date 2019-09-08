package movies.io
import org.apache.spark.sql.DataFrame

import scala.util.Try

class ConsoleWriter extends Writer {
  override def writeDataFrame(dataFrame: DataFrame): Try[Unit] = Try(dataFrame.show())
}
