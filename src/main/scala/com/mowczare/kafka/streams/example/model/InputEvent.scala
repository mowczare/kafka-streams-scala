package com.mowczare.kafka.streams.example.model

import com.avsystem.commons.serialization.HasGenCodec

final case class InputEvent(value: Long) extends AnyVal
object InputEvent extends HasGenCodec
