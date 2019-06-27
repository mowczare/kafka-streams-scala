package com.mowczare.kafka.streams.pds.bloomFilter

import com.avsystem.commons.serialization.GenCodec
import com.google.common.hash.{Funnel, PrimitiveSink}
import com.mowczare.kafka.streams.pds.hashing.{AsByteArray, PrimitiveSinkOutput}

class GenFunnel[T](implicit codec: GenCodec[T])
  extends Funnel[T] {

  override def funnel(from: T, into: PrimitiveSink): Unit = {
    codec.write(new PrimitiveSinkOutput(into), from)
  }
}

class AsByteArrayFunel[T](implicit asbyteArray: AsByteArray[T]) extends Funnel[T] {
  override def funnel(from: T, into: PrimitiveSink): Unit = {
    into.putBytes(asbyteArray.byteArray(from))
  }
}