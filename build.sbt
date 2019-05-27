name := "kafka-streams-scala"

lazy val root = (project in file(".")).settings(
  inThisBuild(List(
    organization := "com.mowczare",
    scalaVersion := "2.12.8",
    version := "0.1"
  )),
  name := "kafka-streams-scala",
  libraryDependencies ++= Dependencies.all
)