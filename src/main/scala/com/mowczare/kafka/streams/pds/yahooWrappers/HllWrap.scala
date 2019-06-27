package com.mowczare.kafka.streams.pds.yahooWrappers

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.AsByteArray
import com.mowczare.kafka.streams.pds.yahooUtils.YahooGenCodecs
import com.yahoo.sketches.hll.HllSketch

case class HllWrap[T](hll: HllSketch)(implicit asByteArray: AsByteArray[T]) {

  def add(elem: T): HllWrap[T] = {
    hll.update(asByteArray.byteArray(elem))
    this
  }

}

object HllWrap {
  import YahooGenCodecs._

  def empty[T: AsByteArray]: HllWrap[T] = new HllWrap(new HllSketch(12))
    //TODO reg num is low level parameter, maybe some more highlevel api for picking good reg num? eg by size?
    //TODO hll has also regwidth parameter, but in most cases picking different number than 4 is waste of memory

  implicit def genCodec[T : AsByteArray]: GenCodec[HllWrap[T]] = GenCodec.transformed[HllWrap[T], HllSketch](
    _.hll,
    new HllWrap(_)
  )

}
