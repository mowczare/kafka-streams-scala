package com.mowczare.kafka.streams.example.stream

import com.avsystem.commons._
import com.mowczare.kafka.streams.StreamOps._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.stream.ExampleStream.streamTopologyHll
import com.mowczare.kafka.streams.hll.hashing.GenCodecHashing._
import com.mowczare.kafka.streams.hll.model.{HllWrap, ItemSketchWrap, ThetaWrap}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Produced

final class ExampleStream(kafkaSettings: KafkaSettings) {

  private val streams: KafkaStreams = new KafkaStreams(
    new StreamsBuilder().setup(streamTopologyHll(kafkaSettings.inputTopic, kafkaSettings.outputTopic)).build(),
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

  def streamTopologyHll(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {

    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[InputEvent] = SerdeUtil.codecToSerde[InputEvent]
    import org.apache.kafka.streams.scala.ImplicitConversions._

    builder
      .stream[String, InputEvent](inputTopic)
      .groupBy { case (key, event) => event.value % 2 }
      .freaquencyXd()
      .toStream
      .peek {case (k, v) => println(k, v)}
      .to(outputTopic)(implicitly[Produced[Long, ItemSketchWrap[InputEvent]]])
  }
}