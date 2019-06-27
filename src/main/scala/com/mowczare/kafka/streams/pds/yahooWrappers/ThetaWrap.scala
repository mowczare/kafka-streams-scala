package com.mowczare.kafka.streams.pds.yahooWrappers

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.AsByteArray
import com.mowczare.kafka.streams.pds.yahooUtils.YahooGenCodecs
import com.yahoo.sketches.theta.UpdateSketch

case class ThetaWrap[T](updateSketch: UpdateSketch)(
    implicit asByteArray: AsByteArray[T]
) {

  def add(elem: T): ThetaWrap[T] = {
    updateSketch.update(asByteArray.byteArray(elem))
    this
  }
}

object ThetaWrap {
  import YahooGenCodecs._

  def empty[T: AsByteArray]: ThetaWrap[T] =
    ThetaWrap(UpdateSketch.builder.build())

  implicit def genCodec[T: AsByteArray]: GenCodec[ThetaWrap[T]] =
    GenCodec.transformed[ThetaWrap[T], UpdateSketch](
      _.updateSketch,
      ThetaWrap(_)
    )
}
