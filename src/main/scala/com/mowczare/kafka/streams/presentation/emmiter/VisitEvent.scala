package com.mowczare.kafka.streams.presentation.emmiter

import com.avsystem.commons.misc.Timestamp
import com.avsystem.commons.serialization.HasGenCodec

case class VisitEvent(person: Person, timestamp: Timestamp)
object VisitEvent extends HasGenCodec[VisitEvent]