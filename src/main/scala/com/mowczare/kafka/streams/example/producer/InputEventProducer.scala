package com.mowczare.kafka.streams.example.producer

import java.util.Properties
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.avsystem.commons._
import com.mowczare.kafka.streams.example.environment.KafkaSettings
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil.codecToSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


final class InputEventProducer(kafkaSettings: KafkaSettings) {

  private val props = new Properties {
    put("bootstrap.servers", kafkaSettings.bootstrapServers)
    put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  }

  private val producer = new KafkaProducer[String, Array[Byte]](props)

  private val entityIds: ISeq[String] = (1 to 4).map(i => s"Entity-$i")

  private def inputEvent(): InputEvent = {
    InputEvent(System.currentTimeMillis())
  }

  private def inputRecord(id: String): ProducerRecord[String, Array[Byte]] = {
    new ProducerRecord[String, Array[Byte]](
      kafkaSettings.inputTopic,
      id,
      codecToSerde[InputEvent].serializer().serialize("", inputEvent())
    )
  }

  private val continuousInputEvents: Iterator[ProducerRecord[String, Array[Byte]]] = {
    Iterator.continually(entityIds.iterator.map(inputRecord)).flatten
  }

  def produceInputEvents(eventsPerSec: Long): ScheduledFuture[_] = {
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
      () => producer.send(continuousInputEvents.next()),
      TimeUnit.SECONDS.toNanos(0),
      TimeUnit.SECONDS.toNanos(1) / eventsPerSec,
      TimeUnit.NANOSECONDS
    )
  }
}
