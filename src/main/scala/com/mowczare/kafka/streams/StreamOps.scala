package com.mowczare.kafka.streams

import com.mowczare.kafka.streams.StreamOps.KGroupedStreamExt
import com.mowczare.kafka.streams.hll.hashing.AsByteArray
import com.mowczare.kafka.streams.hll.model.HllWrap
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable, Materialized}

trait StreamOps {
  implicit def kGroupedStreamExt[KR : Serde, V: AsByteArray](groupedStream: KGroupedStream[KR, V]) = new
    KGroupedStreamExt[KR, V](groupedStream)
}

object StreamOps extends StreamOps {


  class KGroupedStreamExt[KR : Serde, V: AsByteArray](groupedStream: KGroupedStream[KR, V]) {

    implicit val hllSerde : Serde[HllWrap[V]] = implicitly

    def hllXd(): KTable[KR, HllWrap[V]] = {
      groupedStream
        .aggregate(initializer = HllWrap.empty[V]){
          case (kr, v, hll) => hll.add(v)
        }(Materialized.`with`)
    }
  }


}
