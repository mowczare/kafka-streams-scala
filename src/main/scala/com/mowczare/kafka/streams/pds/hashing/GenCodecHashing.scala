package com.mowczare.kafka.streams.pds.hashing

import com.avsystem.commons.serialization.GenCodec

import scala.collection.mutable.ListBuffer

object GenCodecHashing {

  implicit def hasByteArrayContent[T](implicit genCodec: GenCodec[T]): HasByteArrayContent[T] = (t: T) => {
    val accumulator = new ListBuffer[Byte]()
    genCodec.write(new Value2BytesOutput(accumulator), t)
    accumulator.toArray
  }

  implicit def funnel[T: GenCodec]: GenFunnel[T] = new GenFunnel[T]()

}
