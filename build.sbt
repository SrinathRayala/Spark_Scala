name := "untitled1"

version := "0.1"

scalaVersion := "2.11.6"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"