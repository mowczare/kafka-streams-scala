package com.mowczare.kafka.streams.pds.bloomFilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.avsystem.commons.serialization.GenCodec
import com.google.common.hash.{BloomFilter, Funnel}
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.pds.hashing.AsByteArray
import org.apache.kafka.common.serialization.Serde

object BloomFilterUtils {

  def funnel[T: GenCodec]: Funnel[T] = new GenFunnel[T]()

  def asByteArrayFunnel[T : AsByteArray]: Funnel[T] = new AsByteArrayFunel[T]

  def create[T: GenCodec](config: FilterConfig): BloomFilter[T] =
    BloomFilter.create(funnel[T], config.estimatedSize, config.expectedFpp)

  def bloomFilterCodec[T](implicit funnel: Funnel[T]): GenCodec[BloomFilter[T]] = {
    GenCodec.transformed[BloomFilter[T], Array[Byte]](
      filter => { val baos = new ByteArrayOutputStream; filter.writeTo(baos); baos.toByteArray },
      data => BloomFilter.readFrom(new ByteArrayInputStream(data), funnel)
    )
  }

  def bloomFilterCodecFromAsByteArray[T](implicit asByteArray: AsByteArray[T]): GenCodec[BloomFilter[T]] = {
    GenCodec.transformed[BloomFilter[T], Array[Byte]](
      filter => { val baos = new ByteArrayOutputStream; filter.writeTo(baos); baos.toByteArray },
      data => BloomFilter.readFrom(new ByteArrayInputStream(data), asByteArrayFunnel)
    )
  }

  def serde[T : AsByteArray]: Serde[BloomFilter[T]] = SerdeUtil.codecToSerde(bloomFilterCodecFromAsByteArray)

}
