package com.mowczare.kafka.streams.example

import com.mowczare.kafka.streams.example.environment.{KafkaEnvironment, KafkaSettings}
import com.mowczare.kafka.streams.example.stream.ExampleStream

object Launcher extends App {

  lazy val kafkaEnvironment = new KafkaEnvironment(kafkaSettings)
  val shouldClearState = true
  val shouldRunStream = true
  val kafkaSettings = KafkaSettings(
    bootstrapServers = "localhost:9092",
    localStateDir = "/tmp/kafka-streams",
    streamThreads = 4,
    partitions = 4,
    replicationFactor = 1,
    inputTopic = "kafka-streams-input",
    outputTopic = "kafka-streams-output",
  )
  val stream = new ExampleStream(kafkaSettings)

  if (shouldClearState) {
    stream.cleanUp()
    kafkaEnvironment.cleanupKafkaTopics(stream.streamName)
  }

  if (shouldRunStream) {
    kafkaEnvironment.createTopics()
    stream.start()
  }

}
