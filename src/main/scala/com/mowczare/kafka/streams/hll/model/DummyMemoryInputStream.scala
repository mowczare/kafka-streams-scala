package com.mowczare.kafka.streams.hll.model

import java.io.{DataInputStream, InputStream}

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
      case Failure(_) =>
        println("xddd")
        -1
    }

  }
}

//class MemoryDataInputStream(memory: Memory) extends DataInputStream