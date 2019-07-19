package com.mowczare.kafka.streams.presentation.emmiter

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import java.util.{Properties, TimeZone}

import com.avsystem.commons.ISeq
import com.avsystem.commons.misc.Timestamp
import com.avsystem.commons.serialization.HasGenCodec
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.serde.SerdeUtil.codecToSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.{DateTimeZone, LocalDateTime}

case class Person(name: String, surname: String)
object Person extends HasGenCodec[Person]

class DataGenerator() {

  private val names = (1 to 10000).map(_.toString)
  private val surnames = (1 to 10000).map(_.toString)

  private val population = (names zip surnames).map {
    case (name, surname) => Person(name, surname)
  }

  private def generatePerson(int: Int): Person =
    Person(int.toString, int.toString)

  private def generateTimestamp(day: Int): Timestamp =
    Timestamp(
      new LocalDateTime(2018, 12, day).toDateTime(DateTimeZone.UTC).getMillis
    )

  private val weekVisits = List(1234, 543, 2341, 543, 3215, 12543, 43243)

  def iterator: Iterator[VisitEvent] =
    weekVisits.zipWithIndex.iterator.flatMap {
      case (num, day) =>
        (1 to num)
          .map(generatePerson)
          .map(p => VisitEvent(p, generateTimestamp(day)))
    }
}

class DataProducer(kafkaSettings: KafkaSettings, generator: DataGenerator) {

  private val props = new Properties {
    put("bootstrap.servers", kafkaSettings.bootstrapServers)
    put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    put(
      "value.serializer",
      "org.apache.kafka.common.serialization.ByteArraySerializer"
    )
  }

  private val producer = new KafkaProducer[String, Array[Byte]](props)

  private val entityIds: ISeq[String] = (1 to 1000).map(i => s"Entity-$i")

  private def inputRecord(
      event: VisitEvent
  ): ProducerRecord[String, Array[Byte]] = {
    new ProducerRecord[String, Array[Byte]](
      kafkaSettings.inputTopic,
      null,
      codecToSerde[VisitEvent].serializer().serialize("", event)
    )
  }

  private val inputEvents: Iterator[ProducerRecord[String, Array[Byte]]] = {
    generator.iterator.map(inputRecord)
  }

  def produceInputEvents(eventsPerSec: Long): ScheduledFuture[_] = {
    Executors
      .newScheduledThreadPool(1)
      .scheduleAtFixedRate(
        () =>
          if (inputEvents.hasNext) {
            producer.send(inputEvents.next())
          },
        TimeUnit.SECONDS.toNanos(0),
        TimeUnit.SECONDS.toNanos(1) / eventsPerSec,
        TimeUnit.NANOSECONDS
      )
  }

}
