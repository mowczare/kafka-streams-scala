package com.mowczare.kafka.streams.pds.hashing

import com.avsystem.commons.serialization.GenCodec

import scala.collection.mutable.ListBuffer

object GenCodecHashing {

  implicit def asByteArray[T](implicit genCodec: GenCodec[T]): AsByteArray[T] = (t: T) => {
    val accumulator = new ListBuffer[Byte]()
    genCodec.write(new Value2BytesOutput(accumulator), t)
    accumulator.toArray
  }


}
