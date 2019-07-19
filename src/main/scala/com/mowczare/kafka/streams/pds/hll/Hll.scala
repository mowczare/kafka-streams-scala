package com.mowczare.kafka.streams.pds.hll

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.model.UniqueCount
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.hll.{HllSketch, Union}

class Hll[T : HasByteArrayContent](private[hll] val internalHll: HllSketch) {

  def add(elem: T): Hll[T] = {
    internalHll.update(HasByteArrayContent[T].byteArrayContent(elem))
    this
  }

  def estimate: Double = internalHll.getEstimate

  def uniqueCountEstimate: UniqueCount = UniqueCount(estimate.round)

  def asUnion: HllUnion[T] = {
    val union = new Union(internalHll.getLgConfigK)
    union.update(internalHll)
    new HllUnion[T](union)
  }

}

object Hll {
  import YahooGenCodecs._

  def empty[T: HasByteArrayContent](lgRegNum: Int = 12): Hll[T] = new Hll(new HllSketch(lgRegNum))

  implicit def genCodec[T : HasByteArrayContent]: GenCodec[Hll[T]] =
    GenCodec.transformed[Hll[T], HllSketch](
      _.internalHll,
      new Hll(_)
    )

}



