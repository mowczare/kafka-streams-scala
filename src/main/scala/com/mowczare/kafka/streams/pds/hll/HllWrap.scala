package com.mowczare.kafka.streams.pds.hll

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.hll.HllSketch

case class HllWrap[T](hll: HllSketch)(implicit asByteArray: HasByteArrayContent[T]) {

  def add(elem: T): HllWrap[T] = {
    hll.update(asByteArray.byteArrayContent(elem))
    this
  }
}

object HllWrap {
  import YahooGenCodecs._

  def empty[T: HasByteArrayContent]: HllWrap[T] = new HllWrap(new HllSketch(12))

  implicit def genCodec[T : HasByteArrayContent]: GenCodec[HllWrap[T]] = GenCodec.transformed[HllWrap[T], HllSketch](
    _.hll,
    new HllWrap(_)
  )

}
