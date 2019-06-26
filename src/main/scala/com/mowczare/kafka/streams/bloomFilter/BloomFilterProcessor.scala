package com.mowczare.kafka.streams.bloomFilter

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

class BloomFilterProcessor[K, V] extends Processor[K,V] {

  import org.apache.kafka.streams.processor.ProcessorContext
  import org.apache.kafka.streams.state.KeyValueStore

  private var context: ProcessorContext = _
  private var kvStore: KeyValueStore[K, V] = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    import org.apache.kafka.streams.state.KeyValueStore
    kvStore = context.getStateStore("Counts").asInstanceOf[KeyValueStore[K, V]]

    import org.apache.kafka.streams.KeyValue
    import org.apache.kafka.streams.processor.PunctuationType
    import org.apache.kafka.streams.state.KeyValueIterator
    import java.time.Duration
    // schedule a punctuate() method every second based on stream-time
    this.context.schedule(Duration.ofSeconds(1000), PunctuationType.STREAM_TIME, timestamp => {
      def foo(timestamp: Long): Unit = {
        val iter = this.kvStore.all
        while ( {
          iter.hasNext
        }) {
          val entry = iter.next
          context.forward(entry.key, entry.value.toString)
        }
        iter.close()
        // commit the current processing progress
        context.commit()
      }

      foo(timestamp)
    })
  }

  override def process(key: K, value: V): Unit = {}

  override def close(): Unit = {}
}
