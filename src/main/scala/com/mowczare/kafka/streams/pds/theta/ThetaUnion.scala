package com.mowczare.kafka.streams.pds.theta

import com.avsystem.commons.serialization.GenCodec
import com.mowczare.kafka.streams.pds.hashing.HasByteArrayContent
import com.mowczare.kafka.streams.pds.yahooIntegration.serialization.YahooGenCodecs
import com.yahoo.sketches.theta.{SetOperation, Union}

class ThetaUnion[T: HasByteArrayContent](private[theta] val internal: Union) {

  def add(elem: T): ThetaUnion[T] = {
    internal.update(HasByteArrayContent[T].byteArrayContent(elem))
    this
  }

  def union(theta: UpdateTheta[T]): ThetaUnion[T] = {
    internal.update(theta.internal)
    this
  }

  def result: CompactTheta = new CompactTheta(internal.getResult)

}

object ThetaUnion extends YahooGenCodecs {

  def empty[T: HasByteArrayContent]: ThetaUnion[T] =
    new ThetaUnion(SetOperation.builder().buildUnion())

  implicit def genCodec[T: HasByteArrayContent]: GenCodec[ThetaUnion[T]] =
    GenCodec.transformed[ThetaUnion[T], Union](
      _.internal,
      new ThetaUnion[T](_)
    )

}
