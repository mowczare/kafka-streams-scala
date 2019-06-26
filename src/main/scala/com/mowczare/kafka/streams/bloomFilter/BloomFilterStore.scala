package com.mowczare.kafka.streams.bloomFilter

import com.google.common.hash.BloomFilter
import com.mowczare.kafka.streams.hll.hashing.AsByteArray
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

object BloomFilterStore {

  def bloomFilterStore[K : Serde, V : AsByteArray](config: FilterConfig): StoreBuilder[KeyValueStore[K, BloomFilter[V]]] =
    Stores.keyValueStoreBuilder[K, BloomFilter[V]](
    Stores.persistentKeyValueStore("TODO"),
      implicitly[Serde[K]],
      BloomFilterUtils.serde[V]
    ).withLoggingDisabled()


}
