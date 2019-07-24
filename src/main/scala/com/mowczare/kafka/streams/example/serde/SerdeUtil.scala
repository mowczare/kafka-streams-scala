package com.mowczare.kafka.streams.example.serde

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import com.avsystem.commons._
import com.avsystem.commons.serialization.{GenCodec, StreamInput, StreamOutput}
import com.avsystem.commons.serialization.json.{JsonDateFormat, JsonOptions, JsonStringInput, JsonStringOutput}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

object SerdeUtil {

  /** Creates a Kafka Serde for provided type with the `GenCodec` defined. */
  implicit def codecToSerde[T >: Null : GenCodec]: Serde[T] = {
    val jsonOptions = JsonOptions(dateFormat = JsonDateFormat.EpochMillis)

    val serializer = new Serializer[T] {
      override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

      override def close(): Unit = ()

      override def serialize(topic: String, data: T): Array[Byte] = {
        val array = new ByteArrayOutputStream()
        GenCodec[T].write(new StreamOutput(new DataOutputStream(array)), data)
        array.toByteArray
      }
    }

    val deserializer = new Deserializer[T] {
      override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

      override def close(): Unit = ()

      override def deserialize(topic: String, data: Array[Byte]): T = {
        val input = new ByteArrayInputStream(data)
        GenCodec[T].read(new StreamInput(new DataInputStream(input)))
      }
    }
    Serdes.serdeFrom(serializer, deserializer)
  }
}