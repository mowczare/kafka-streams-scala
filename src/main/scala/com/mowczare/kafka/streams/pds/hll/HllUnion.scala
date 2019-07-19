package com.mowczare.kafka.streams.pds.hll

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.model.UniqueCount
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.hll.{HllSketch, Union}

class HllUnion[T: HasByteArrayContent](private[hll] val internalHll: Union) {

  def merge(hll: Hll[T]): HllUnion[T] = {
    internalHll.update(hll.internalHll)
    this
  }
  def add(elem: T): HllUnion[T] = {
    internalHll.update(HasByteArrayContent[T].byteArrayContent(elem))
    this
  }

  def estimate: Double = internalHll.getEstimate

  def uniqueCountEstimate: UniqueCount = UniqueCount(estimate.round)

}

object HllUnion extends YahooGenCodecs {
  def empty[T: HasByteArrayContent](lgRegNum: Int = 12) =
    new HllUnion(new Union(lgRegNum))

  implicit def genCodec[T: HasByteArrayContent]: GenCodec[HllUnion[T]] =
    GenCodec.transformed[HllUnion[T], Union](
      _.internalHll,
      new HllUnion(_)
    )
}
