name := "TFI-DataPreprocessor"

version := "4.2.0"

scalaVersion := "2.12.11"

//Runtime Libraries
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "com.databricks" %% "dbutils-api" % "0.0.4"

//Testing Libraries
libraryDependencies +="com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

//Testing Settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false
