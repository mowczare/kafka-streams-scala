package com.mowczare.kafka.streams.bloomFilter

import com.mowczare.kafka.streams.example.serde.SerdeUtil._
import com.mowczare.kafka.streams.hll.hashing.GenCodecHashing._
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}

object ExampleTopology {

  import org.apache.kafka.streams.Topology

  val builder = new Topology

//  builder
//    .addSource("Source", "source-topic")
//    .addProcessor("Process", new ProcessorSupplier[_, _] {
//      override def get(): Processor[_, _] =
//        new BloomFilterProcessor[String, String]
//    }, "Source")
//    .addStateStore(
//      BloomFilterStore.bloomFilterStore[String, String](FilterConfig(1, 2)),
//      "Process"
//    )
//    .addSink("Sink", "sink-topic", "Process")

}
