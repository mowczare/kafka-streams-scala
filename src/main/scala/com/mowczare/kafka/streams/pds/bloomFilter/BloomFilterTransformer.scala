package com.mowczare.kafka.streams.pds.bloomFilter

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

class BloomFilterTransformer[K, V, R] extends Transformer[K, V, R] {
  override def init(context: ProcessorContext): Unit = ???

  override def transform(key: K, value: V): R = ???

  override def close(): Unit = ???
}
