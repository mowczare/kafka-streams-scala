package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.avsystem.commons.jiop.JavaInterop.JList
import com.avsystem.commons.serialization.{GenCodec, StreamInput, StreamOutput}
import com.yahoo.memory.Memory
import com.avsystem.commons.jiop.JavaInterop._
import scala.reflect.ClassTag

class GenCodecArrayOfItemsSerDe[T: ClassTag: GenCodec]
    extends ScalaArrayOfItemsSerde[T] {
  private val listGenCodec = implicitly[GenCodec[List[T]]]

  override def serializeToByteArray(items: JList[T]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = new StreamOutput(
      new DataOutputStream(byteArrayOutputStream)
    )
    listGenCodec.write(outputStream, items.asScala.toList)
    byteArrayOutputStream.toByteArray
  }

  override def deserializeFromMemory(mem: Memory, numItems: Int): JList[T] = {
    {
      val input = new StreamInput(
        new DataInputStream(new DummyMemoryInputStream(mem))
      )
      listGenCodec.read(input).to[JList]
    }

  }
}
