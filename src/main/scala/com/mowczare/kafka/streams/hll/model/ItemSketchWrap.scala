package com.mowczare.kafka.streams.hll.model

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets

import com.avsystem.commons.serialization.{GenCodec, StreamInput, StreamOutput}
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.hll.hashing.AsByteArray
import com.yahoo.memory.{Memory, UnsafeUtil}
import com.yahoo.sketches.{ArrayOfItemsSerDe, ArrayOfStringsSerDe}
import com.yahoo.sketches.frequencies.ErrorType
import com.yahoo.sketches.frequencies.ItemsSketch
import com.yahoo.sketches.theta.{Sketches, UpdateSketch}
import org.apache.kafka.common.serialization.Serde
import com.avsystem.commons.jiop.JavaInterop._
import com.avsystem.commons.serialization.json.{JsonStringInput, JsonStringOutput}

import scala.reflect.ClassTag


case class ItemSketchWrap[T](itemSketch: ItemsSketch[T]) {

  def add(elem: T): ItemSketchWrap[T] = {
    itemSketch.update(elem)
    this
  }

  def add(elem: T, count: Long): ItemSketchWrap[T] = {
    itemSketch.update(elem, count)
    this
  }
}


object ItemSketchWrap {

  implicit def itemSketchGencodec[T : GenCodec: ClassTag]: GenCodec[ItemsSketch[T]] =
    GenCodec.transformed[ItemsSketch[T], Array[Byte]](_.toByteArray(new DataSerdeWrap(new GencodecArrayOfItemsSerDe[T]())),
      byteArray =>
        ItemsSketch.getInstance(Memory.wrap(byteArray), new DataSerdeWrap(new GencodecArrayOfItemsSerDe[T]()))
    )


  def empty[T]: ItemSketchWrap[T] = ItemSketchWrap(new ItemsSketch[T](64)) //some capacity move it to api in the future


  implicit def genCodec[T : GenCodec : ClassTag]: GenCodec[ItemSketchWrap[T]] = GenCodec.transformed[ItemSketchWrap[T], ItemsSketch[T]](
    _.itemSketch,
    ItemSketchWrap(_)
  )

  implicit def thetaSerde[T : GenCodec : ClassTag]: Serde[ItemSketchWrap[T]] = SerdeUtil.codecToSerde[ItemSketchWrap[T]]

}

class GencodecArrayOfItemsSerDe[T : ClassTag](implicit genCodec: GenCodec[T]) extends ScalaArrayOfitemsSerde[T] {
  val arrayGenCodec =  implicitly[GenCodec[List[T]]]

  override def serializeToByteArray(items: JList[T]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = new StreamOutput(new DataOutputStream(byteArrayOutputStream))
    arrayGenCodec.write(outputStream, items.asScala.toList)
    byteArrayOutputStream.toByteArray
//      .to[JList]
  }

  override def deserializeFromMemory(mem: Memory, numItems: Int): JList[T] = {
    {
      val input = new StreamInput(new DataInputStream(new DummyMemoryInputStream(mem)))
      arrayGenCodec.read(input).to[JList]
    }

  }
}

class JsonGencodecArrayOfItemsSerDe[T : ClassTag](implicit genCodec: GenCodec[T]) extends ScalaArrayOfitemsSerde[T] {
  val serde = new ArrayOfStringsSerDe

  val arrayGenCodec =  implicitly[GenCodec[List[T]]]

  override def serializeToByteArray(items: JList[T]): Array[Byte] = {
    serde.serializeToByteArray(items.asScala.map(v => JsonStringOutput.write(v)).toArray)
//    val byteArrayOutputStream = new ByteArrayOutputStream()
//    val outputStream = new StreamOutput(new DataOutputStream(byteArrayOutputStream))
//    arrayGenCodec.write(outputStream, items.asScala.toList)
//    byteArrayOutputStream.toByteArray.to[JList]
  }

  override def deserializeFromMemory(mem: Memory, numItems: Int): JList[T] = {
    {
      serde.deserializeFromMemory(mem, numItems).iterator.map(str => JsonStringInput.read(str)).to[JList]
//      val input = new StreamInput(new DataInputStream(new DummyMemoryInputStream(mem)))
//      arrayGenCodec.read(input).to[JList]
    }

  }
}