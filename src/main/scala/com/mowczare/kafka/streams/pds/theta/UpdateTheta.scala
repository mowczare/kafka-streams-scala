package com.mowczare.kafka.streams.pds.theta

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.model.UniqueCount
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.theta.UpdateSketch

class UpdateTheta[T: HasByteArrayContent](
    val internal: UpdateSketch
) {

  def add(elem: T): UpdateTheta[T] = {
    internal.update(HasByteArrayContent[T].byteArrayContent(elem))
    this
  }

  def estimate: Double = internal.getEstimate

  def uniqueCountEstimate: UniqueCount = UniqueCount(internal.getEstimate.round)

}

object UpdateTheta extends YahooGenCodecs {

  def empty[T: HasByteArrayContent]: UpdateTheta[T] =
    new UpdateTheta(UpdateSketch.builder.build())

  implicit def genCodec[T: HasByteArrayContent]: GenCodec[UpdateTheta[T]] =
    GenCodec.transformed[UpdateTheta[T], UpdateSketch](
      _.internal,
      new UpdateTheta(_)
    )
}
