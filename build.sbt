name := "spark_etl_api"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(

  "org.postgresql" % "postgresql" % "9.3-1101-jdbc4",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
)