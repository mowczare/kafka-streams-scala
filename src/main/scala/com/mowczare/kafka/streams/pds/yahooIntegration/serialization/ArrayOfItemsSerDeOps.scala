package com.mowczare.kafka.streams.pds.yahooIntegration.serialization

import com.avsystem.commons.serialization.GenCodec

object ArrayOfItemsSerDeOps {
  def arrayOfItemsSerDe[T : GenCodec] = new DataSerdeScalaWrap(new GenCodecArrayOfItemsSerDe[T]())
}
