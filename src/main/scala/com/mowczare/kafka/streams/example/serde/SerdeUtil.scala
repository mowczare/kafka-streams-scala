package com.mowczare.kafka.streams.example.serde

import java.nio.charset.StandardCharsets

import com.avsystem.commons._
import com.avsystem.commons.serialization.GenCodec
import com.avsystem.commons.serialization.json.{JsonDateFormat, JsonOptions, JsonStringInput, JsonStringOutput}
import com.yahoo.sketches.hll.HllSketch
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

object SerdeUtil {

  implicit val hllGenCodec: GenCodec[HllSketch] =
    GenCodec.transformed[HllSketch, Array[Byte]](_.toCompactByteArray, HllSketch.heapify)

  /** Creates a Kafka Serde for provided type with the `GenCodec` defined. */
  implicit def codecToSerde[T >: Null : GenCodec]: Serde[T] = {
    val jsonOptions = JsonOptions(dateFormat = JsonDateFormat.EpochMillis)

    val serializer = new Serializer[T] {
      override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

      override def close(): Unit = ()

      override def serialize(topic: String, data: T): Array[Byte] = {
        JsonStringOutput.write(data, jsonOptions).getBytes(StandardCharsets.UTF_8)
      }
    }
    val deserializer = new Deserializer[T] {
      override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

      override def close(): Unit = ()

      override def deserialize(topic: String, data: Array[Byte]): T = {
        data.opt.map(d => JsonStringInput.read[T](new String(d, StandardCharsets.UTF_8), jsonOptions)).orNull
      }
    }
    Serdes.serdeFrom(serializer, deserializer)
  }


}
