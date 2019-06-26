package com.mowczare.kafka.streams.hll.model

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.example.serde.SerdeUtil._
import com.mowczare.kafka.streams.hll.hashing.AsByteArray
import com.twitter.algebird._
import com.yahoo.sketches.hll.{HllSketch, Union}
import org.apache.kafka.common.serialization.Serde

case class HllWrap[T](hll: HllSketch)(implicit asByteArray: AsByteArray[T]) {

  def add(elem: T): HllWrap[T] = {
    val copy = hll.copy()
    copy.update(asByteArray.byteArray(elem))
    new HllWrap[T](copy)
  }

}

object HllWrap {

  val monoid = new HyperLogLogMonoid(12)

  def empty[T: AsByteArray]: HllWrap[T] = new HllWrap(new HllSketch(12))

  def add[T: AsByteArray](l: HllWrap[T], r: HllWrap[T]): HllWrap[T] =
    HllWrap({val union = new Union(12); union.update(r.hll); union.getResult})

  implicit def genCodec[T : AsByteArray]: GenCodec[HllWrap[T]] = GenCodec.transformed[HllWrap[T], HllSketch](
    _.hll,
    new HllWrap(_)
  )

  implicit def hllSerde[T : AsByteArray]: Serde[HllWrap[T]] = SerdeUtil.codecToSerde[HllWrap[T]]
}
