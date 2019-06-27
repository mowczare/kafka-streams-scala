package com.mowczare.kafka.streams.pds.yahooUtils.serialization

import java.io.InputStream

import com.yahoo.memory.{Memory, UnsafeUtil}

import scala.util.{Failure, Success, Try}

class DummyMemoryInputStream(memory: Memory) extends InputStream {
  var offsetBytes = 0
  override def read(): Int = {
    Try {
      UnsafeUtil.checkBounds(offsetBytes, 1, memory.getCapacity)
    } match {
      case Success(_) =>
        val res = memory.getByte(offsetBytes)
        offsetBytes += 1
        res
      case Failure(_) => -1
    }
  }
}