package com.mowczare.kafka.streams.pds.hashing

import com.avsystem.commons.serialization.GenCodec
import com.google.common.hash.{Funnel, PrimitiveSink}

class GenFunnel[T](implicit codec: GenCodec[T])
  extends Funnel[T] {

  override def funnel(from: T, into: PrimitiveSink): Unit = {
    codec.write(new PrimitiveSinkOutput(into), from)
  }
}

class HasByteArrayContentFunel[T](implicit hasByteArrayContent: HasByteArrayContent[T]) extends Funnel[T] {
  override def funnel(from: T, into: PrimitiveSink): Unit = {
    into.putBytes(hasByteArrayContent.byteArrayContent(from))
  }
}