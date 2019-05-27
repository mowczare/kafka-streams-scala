package com.mowczare.kafka.streams.example.environment

import java.util.Properties

import com.avsystem.commons._
import org.apache.kafka.streams.StreamsConfig

final case class KafkaSettings(bootstrapServers: String,
                               localStateDir: String,
                               streamThreads: Int,
                               partitions: Int,
                               replicationFactor: Int,
                               inputTopic: String,
                               outputTopic: String) {

  def streamProperties(streamName: String): Properties = new Properties().setup { props =>
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(StreamsConfig.STATE_DIR_CONFIG, localStateDir)
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads.toString)
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor.toString)
  }
}

