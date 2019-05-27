package com.mowczare.kafka.streams.example.model

import com.avsystem.commons.serialization.HasGenCodec

final case class InputEvent(value: Long)
object InputEvent extends HasGenCodec[InputEvent]
