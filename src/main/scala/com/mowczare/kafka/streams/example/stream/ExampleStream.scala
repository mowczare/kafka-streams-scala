package com.mowczare.kafka.streams.example.stream

import com.avsystem.commons._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.stream.ExampleStream.streamTopology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

final class ExampleStream(kafkaSettings: KafkaSettings) {

  private val streams: KafkaStreams = new KafkaStreams(
    new StreamsBuilder().setup(streamTopology(kafkaSettings.inputTopic, kafkaSettings.outputTopic)).build(),
    kafkaSettings.streamProperties(streamName)
  )

  def streamName: String = "ExampleStream"

  def start(): Unit = streams.start()

  def cleanUp(): Unit = streams.cleanUp()
}

object ExampleStream {
  def streamTopology(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {

    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[InputEvent] = SerdeUtil.codecToSerde[InputEvent]
    import org.apache.kafka.streams.scala.ImplicitConversions._

    //TODO add your streaming topology logic here:
    builder
      .stream[String, InputEvent](inputTopic)
      .filter((_, _) => false)
      .to(outputTopic)
  }
}