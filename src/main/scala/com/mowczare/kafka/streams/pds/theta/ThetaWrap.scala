package com.mowczare.kafka.streams.pds.theta

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.theta.UpdateSketch

case class ThetaWrap[T](updateSketch: UpdateSketch)(
    implicit asByteArray: HasByteArrayContent[T]
) {

  def add(elem: T): ThetaWrap[T] = {
    updateSketch.update(asByteArray.byteArrayContent(elem))
    this
  }
}

object ThetaWrap {
  import YahooGenCodecs._

  def empty[T: HasByteArrayContent]: ThetaWrap[T] =
    ThetaWrap(UpdateSketch.builder.build())

  implicit def genCodec[T: HasByteArrayContent]: GenCodec[ThetaWrap[T]] =
    GenCodec.transformed[ThetaWrap[T], UpdateSketch](
      _.updateSketch,
      ThetaWrap(_)
    )
}
