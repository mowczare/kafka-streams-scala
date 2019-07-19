package com.mowczare.kafka.streams.pds.hashing

trait HasByteArrayContent[T] {
  def byteArrayContent(t: T): Array[Byte]
}
object HasByteArrayContent {
  def apply[T](implicit evidence: HasByteArrayContent[T]): HasByteArrayContent[T] = evidence
}

