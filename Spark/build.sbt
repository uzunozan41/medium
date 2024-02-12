ThisBuild / version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.10"
libraryDependencies +="org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.1.2"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"

libraryDependencies += "joda-time" % "joda-time" % "2.2"
libraryDependencies += "org.joda" % "joda-convert" % "2.2.2"