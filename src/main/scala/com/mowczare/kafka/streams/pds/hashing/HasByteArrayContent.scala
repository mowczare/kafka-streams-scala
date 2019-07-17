package com.mowczare.kafka.streams.pds.hashing

trait HasByteArrayContent[T] {
  def byteArrayContent(t: T): Array[Byte]
}

