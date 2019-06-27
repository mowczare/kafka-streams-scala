package com.mowczare.kafka.streams.pds.yahooUtils.serialization

import com.avsystem.commons.jiop.JavaInterop.JList
import com.yahoo.memory.Memory

trait ScalaArrayOfItemsSerde[T] {

  def serializeToByteArray(items: JList[T]): Array[Byte]
  def deserializeFromMemory(mem: Memory, numItems: Int): JList[T]

}
