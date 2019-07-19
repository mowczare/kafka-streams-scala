package com.mowczare.kafka.streams

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.StreamOps.{KGroupedStreamExt, KGroupedStreamExtVGenCodec}
import com.mowczare.kafka.streams.example.serde.SerdeUtil._
import com.mowczare.kafka.streams.pds.frequency.ItemSketchWrap
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.hll.Hll
import com.mowczare.kafka.streams.pds.theta.UpdateTheta
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable}

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

    def hll(): KTable[KR, Hll[V]] = {
      groupedStream
        .aggregate(initializer = Hll.empty[V]()) {
          case (kr, v, hll) => hll.add(v)
        }
    }

    def theta(): KTable[KR, UpdateTheta[V]] = {
      groupedStream
        .aggregate(initializer = UpdateTheta.empty[V]) {
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
