package com.mowczare.kafka.streams.pds.model

import com.avsystem.commons.serialization.HasGenCodec

case class UniqueCount(count: Long)
object UniqueCount extends HasGenCodec[UniqueCount]