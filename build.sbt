ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

val sparkVersion = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-core" %sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" %sparkVersion

lazy val root = (project in file("."))
  .settings(
    name := "scalaExample"
  )
