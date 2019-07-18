package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import com.avsystem.commons.serialization.GenCodec

import scala.reflect.ClassTag

object ArrayOfItemsSerDeOps {
  def arrayOfItemsSerDe[T : GenCodec : ClassTag] =
    new DataSerdeScalaWrap(new GenCodecArrayOfItemsSerDe[T]())
}
