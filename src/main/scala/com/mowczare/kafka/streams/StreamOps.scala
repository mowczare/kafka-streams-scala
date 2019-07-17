package com.mowczare.kafka.streams

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.StreamOps.{KGroupedStreamExt, KGroupedStreamExtVGenCodec}
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable}
import com.mowczare.kafka.streams.example.serde.SerdeUtil._
import com.mowczare.kafka.streams.pds.frequency.ItemSketchWrap
import com.mowczare.kafka.streams.pds.hll.HllWrap
import com.mowczare.kafka.streams.pds.theta.ThetaWrap

import scala.reflect.ClassTag

trait StreamOps {

  implicit def kGroupedStreamExt[KR: Serde, V: HasByteArrayContent](
      groupedStream: KGroupedStream[KR, V]
  ): KGroupedStreamExt[KR, V] = new KGroupedStreamExt[KR, V](groupedStream)

  implicit def kGroupedStreamExtGEncodec[
      KR: Serde,
      V <: AnyRef: GenCodec: ClassTag
  ](groupedStream: KGroupedStream[KR, V]): KGroupedStreamExtVGenCodec[KR, V] =
    new KGroupedStreamExtVGenCodec[KR, V](groupedStream)
}

object StreamOps extends StreamOps {

  import org.apache.kafka.streams.scala.ImplicitConversions._

  class KGroupedStreamExt[KR: Serde, V: HasByteArrayContent](
      groupedStream: KGroupedStream[KR, V]
  ) {

    def hll(): KTable[KR, HllWrap[V]] = {
      groupedStream
        .aggregate(initializer = HllWrap.empty[V]) {
          case (kr, v, hll) => hll.add(v)
        }
    }

    def theta(): KTable[KR, ThetaWrap[V]] = {
      groupedStream
        .aggregate(initializer = ThetaWrap.empty[V]) {
          case (kr, v, hll) => hll.add(v)
        }
    }

  }

  class KGroupedStreamExtVGenCodec[KR: Serde, V <: AnyRef: GenCodec: ClassTag](
      groupedStream: KGroupedStream[KR, V]
  ) {

    def frequency(capacity: Int): KTable[KR, ItemSketchWrap[V]] = {
      groupedStream
        .aggregate(initializer = ItemSketchWrap.empty[V](capacity)) {
          case (kr, v, hll) => hll.add(v)
        }
    }
  }

}
