name := "movie-data"

version := "1.0.0"

scalaVersion := "2.11.8"
sparkVersion := "2.2.0"
sparkComponents ++= Seq("sql")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.mockito" % "mockito-core" % "2.8.47" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)