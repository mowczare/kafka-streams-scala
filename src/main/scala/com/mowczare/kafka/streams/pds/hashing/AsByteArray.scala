package com.mowczare.kafka.streams.pds.hashing

trait AsByteArray[T] {
  def byteArray(t: T): Array[Byte]
}

