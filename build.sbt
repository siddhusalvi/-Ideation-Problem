name := "SqlOps"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.20"


