import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

package object movies {

  object Schemas {
    val ratingsSchema = StructType(Array(
      StructField("titleId", StringType),
      StructField("averageRating", DoubleType),
      StructField("numberOfVotes", LongType)
    ))

    val titlesSchema = StructType(Array(
      StructField("titleId", StringType),
      StructField("titleType", StringType),
      StructField("primaryTitle", StringType),
      StructField("originalTitle", StringType),
      StructField("isAdult", StringType),
      StructField("startYear", StringType),
      StructField("endYear", StringType),
      StructField("runtimeMinutes", StringType),
      StructField("genres", StringType)
    ))

    val crewSchema = StructType(Array(
      StructField("titleId", StringType),
      StructField("directorIds", ArrayType(StringType)),
      StructField("writerIds", ArrayType(StringType))
    ))

    val principalsSchema = StructType(Array(
      StructField("titleId", StringType),
      StructField("ordering", IntegerType),
      StructField("principalId", StringType),
      StructField("category", StringType),
      StructField("job", StringType),
      StructField("characters", StringType)
    ))

    val namesSchema = StructType(Array(
      StructField("personId", StringType),
      StructField("primaryName", StringType),
      StructField("birthYear", StringType),
      StructField("deathYear", StringType),
      StructField("primaryProfession", StringType),
      StructField("knownForTitles", StringType)
    ))
  }
}