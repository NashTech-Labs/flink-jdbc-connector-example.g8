name := """flink-jdbc-connector-example"""

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-jdbc" % "1.2.0",
  "org.apache.flink" % "flink-java" % "1.2.0",
  "mysql" % "mysql-connector-java" % "6.0.6",
  "org.apache.flink" % "flink-clients_2.11" % "1.2.0"
)
