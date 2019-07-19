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

  val twitterVersion = "0.13.5"
  val guavaVersion = "19.0"

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
    "com.addthis" % "stream-lib" % streamLibVersion,

    "com.twitter" %% "algebird-core" % twitterVersion,
    "com.google.guava" % "guava" % guavaVersion,
    "com.yahoo.datasketches" % "sketches-core" % "0.13.4",
    "joda-time" % "joda-time" % "2.10.3"
    
  )

  val all: Seq[ModuleID] = kafkaDeps ++ avSystemDeps ++ otherDeps

}
