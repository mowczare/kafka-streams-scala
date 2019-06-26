package com.mowczare.kafka.streams.hll.model
import com.avsystem.commons.jiop.JavaInterop._
import com.yahoo.memory.Memory

trait ScalaArrayOfitemsSerde[T] {

  def serializeToByteArray(items: JList[T]): Array[Byte]
  def deserializeFromMemory(mem: Memory, numItems: Int): JList[T]

}
