package com.mowczare.kafka.streams.hll.hashing

trait AsByteArray[T] {
  def byteArray(t: T): Array[Byte]
}

