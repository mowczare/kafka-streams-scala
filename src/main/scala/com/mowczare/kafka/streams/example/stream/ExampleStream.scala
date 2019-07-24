package com.mowczare.kafka.streams.example.stream

import com.avsystem.commons._
import com.mowczare.kafka.streams.StreamOps._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.stream.ExampleStream.streamTopologyFrequency
import com.mowczare.kafka.streams.pds.frequency.ItemSketchWrap
import com.mowczare.kafka.streams.pds.hll.Hll
import com.mowczare.kafka.streams.pds.theta.UpdateTheta
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Produced

final class ExampleStream(kafkaSettings: KafkaSettings) {

  private val streams: KafkaStreams = new KafkaStreams(
    new StreamsBuilder().setup(streamTopologyFrequency(64)(kafkaSettings.inputTopic, kafkaSettings.outputTopic)).build(),
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
    import SerdeUtil._
    import com.mowczare.kafka.streams.pds.hashing.GenCodecHashing._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    builder
      .stream[String, InputEvent](inputTopic)
      .groupBy { case (key, event) => event.value % 2 }
      .hll()
      .toStream
      .to(outputTopic)(implicitly[Produced[Long, Hll[InputEvent]]])
  }

  def streamTopologyTheta(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {

    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[InputEvent] = SerdeUtil.codecToSerde[InputEvent]
    import SerdeUtil._
    import com.mowczare.kafka.streams.pds.hashing.GenCodecHashing._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    builder
      .stream[String, InputEvent](inputTopic)
      .groupBy { case (key, event) => event.value % 2 }
      .theta()
      .toStream
      .to(outputTopic)(implicitly[Produced[Long, UpdateTheta[InputEvent]]])
  }

  def streamTopologyFrequency(capacity: Int)(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {

    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[InputEvent] = SerdeUtil.codecToSerde[InputEvent]
    import SerdeUtil._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    builder
      .stream[String, InputEvent](inputTopic)
      .groupBy { case (key, event) => event.value % 2 }

      .frequency(capacity)

      .toStream
      .peek{case(i, c) => println(i, c)}
      .to(outputTopic)(implicitly[Produced[Long, ItemSketchWrap[InputEvent]]])
  }
}