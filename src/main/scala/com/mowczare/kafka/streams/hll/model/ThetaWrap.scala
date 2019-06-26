package com.mowczare.kafka.streams.hll.model

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.hll.hashing.AsByteArray
import com.yahoo.memory.Memory
import com.yahoo.sketches.theta.{Sketches, UpdateSketch}
import org.apache.kafka.common.serialization.Serde

case class ThetaWrap[T](updateSketch: UpdateSketch)(implicit asByteArray: AsByteArray[T]) {
  def add(elem: T): ThetaWrap[T] = {
    updateSketch.update(asByteArray.byteArray(elem))
    this
  }
}

object ThetaWrap {
  implicit val thetaGenCodec: GenCodec[UpdateSketch] =
    GenCodec.transformed[UpdateSketch, Array[Byte]](_.toByteArray, byteArray => Sketches.heapifyUpdateSketch(Memory.wrap(byteArray)))


  def empty[T :AsByteArray]: ThetaWrap[T] = ThetaWrap(UpdateSketch.builder.build())


  implicit def genCodec[T : AsByteArray]: GenCodec[ThetaWrap[T]] = GenCodec.transformed[ThetaWrap[T], UpdateSketch](
    _.updateSketch,
    ThetaWrap(_)
  )

  implicit def thetaSerde[T : AsByteArray]: Serde[ThetaWrap[T]] = SerdeUtil.codecToSerde[ThetaWrap[T]]


}