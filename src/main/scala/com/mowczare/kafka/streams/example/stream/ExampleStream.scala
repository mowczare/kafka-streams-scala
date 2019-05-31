package com.mowczare.kafka.streams.example.stream

import java.time.Duration

import com.avsystem.commons._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.stream.ExampleStream.streamTopology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{Suppressed, TimeWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KTable

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

  implicit class EnrichedKTable[K, V](private val inner: KTable[K, V]) extends AnyVal {
    def suppress(suppressed: Suppressed[_ >: K]): KTable[K, V] = new KTable(inner.inner.suppress(suppressed))
  }

  def streamTopology(inputTopic: String, outputTopic: String)(builder: StreamsBuilder): Unit = {

    import org.apache.kafka.streams.scala.Serdes._
    implicit val inputEventSerde: Serde[InputEvent] = SerdeUtil.codecToSerde[InputEvent]
    implicit val outputEventSerde: Serde[(InputEvent, Long, Long)] = SerdeUtil.codecToSerde[(InputEvent, Long, Long)]
    implicit val checkOutputEventSerde: Serde[ISet[Long]] = SerdeUtil.codecToSerde[ISet[Long]]
    import org.apache.kafka.streams.scala.ImplicitConversions._

    val suppressed = builder
      .stream[String, InputEvent](inputTopic)
      .groupByKey
      .windowedBy(TimeWindows
        .of(Duration.ofSeconds(30))
        .advanceBy(Duration.ofSeconds(10))
        .grace(Duration.ofSeconds(10))
      )
      .reduce((_, newOne) => newOne)
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream
      .map { case (windowedId, value) =>
        windowedId.key() -> (value, windowedId.window().start(), windowedId.window().end())
      }

    suppressed
      .groupByKey
      .aggregate[ISet[Long]](ISet.empty) { case (key, (_, start: Long, end: Long), oldAggr: ISet[Long]) =>
      if (oldAggr.contains(start))
        println(s"Found window duplicate: (key: $key, window start: $start, window end: $end")
      oldAggr + start
    }.toStream.to("check")

    suppressed.to(outputTopic)
  }
}