package com.mowczare.kafka.streams.presentation.stream

import com.avsystem.commons.SharedExtensions._
import com.mowczare.kafka.streams.StreamOps._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.stream.ExampleStream.streamTopologyHll
import com.mowczare.kafka.streams.pds.hll.Hll
import com.mowczare.kafka.streams.presentation.emmiter.VisitEvent
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Produced
import org.joda.time.{DateTime, DateTimeZone}

final class UniqueCountStream(kafkaSettings: KafkaSettings) {

  private val streams: KafkaStreams = new KafkaStreams(
    new StreamsBuilder().setup(streamTopologyHll(kafkaSettings.inputTopic, kafkaSettings.outputTopic)).build(),
    kafkaSettings.streamProperties(streamName)
  )

  def streamName: String = "UniqueCountStream"

  def start(): Unit = streams.start()

  def cleanUp(): Unit = streams.cleanUp()
}

object UniqueCountStream {

  def streamTopologyHll(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {
    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[VisitEvent] = SerdeUtil.codecToSerde[VisitEvent]
    import com.mowczare.kafka.streams.pds.hashing.GenCodecHashing._
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import SerdeUtil._

    builder
      .stream[String, VisitEvent](inputTopic)
      .groupBy { case (key, event) => new DateTime(event.timestamp.millis, DateTimeZone.UTC).withTimeAtStartOfDay().getMillis }
      .hll()
      .toStream
      .to(outputTopic)(implicitly[Produced[Long, Hll[VisitEvent]]])


  }


}
