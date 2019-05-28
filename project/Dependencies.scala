import sbt._

object Dependencies {

  val versionOfScala = "2.12.8"

  val kafkaVersion = "2.2.0"

  val logbackVersion = "1.2.3"
  val scalaLoggingVersion = "3.9.2"
  val mockedStreamsVersion = "3.3.0"
  val avsystemCommonsVersion = "1.34.19"
  val scalaTestVersion = "3.0.7"
  val streamLibVersion = "3.0.0"

  private val kafkaDeps = Seq(
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
    "com.madewithtea" %% "mockedstreams" % mockedStreamsVersion % Test,
  )
  private val avSystemDeps = Seq(
    "com.avsystem.commons" %% "commons-core" % avsystemCommonsVersion,
  )
  private val otherDeps = Seq(
    "ch.qos.logback" % "logback-classic" % logbackVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "com.addthis" % "stream-lib" % streamLibVersion
  )

  val all: Seq[ModuleID] = kafkaDeps ++ avSystemDeps ++ otherDeps

}
