package com.mowczare.kafka.streams.example.environment

import java.util.Properties

import com.avsystem.commons._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

final class KafkaEnvironment(kafkaSettings: KafkaSettings) extends StrictLogging {

  private val topics: List[String] = List(kafkaSettings.inputTopic, kafkaSettings.outputTopic)

  private val adminClientProps: Properties = new Properties()
    .setup(_.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSettings.bootstrapServers))

  private val adminClient = AdminClient.create(adminClientProps)

  def createTopics(): Unit = {
    adminClient.createTopics {
      topics.map(newTopic).asJavaCollection
    }.values().asScala.foreach { case (topicName, future) =>
      Try(future.get())
        .fold(
          exception => logger.warn(s"Could not create $topicName topic: ${exception.getMessage}"),
          _ => logger.info(s"Successfully created $topicName topic")
        )
    }
  }

  private def newTopic(topicName: String): NewTopic =
    new NewTopic(topicName, kafkaSettings.partitions, kafkaSettings.replicationFactor.toShort)

  def cleanupKafkaTopics(streamNames: String*): Unit = {
    adminClient.deleteTopics {
      (adminClient.listTopics().names().get().asScala.filter(createdTopic => streamNames.exists(createdTopic.contains))
        ++ topics).asJavaCollection
    }.values().asScala.foreach { case (topicName, future) =>
      Try(future.get())
        .fold(
          exception => logger.warn(s"Could not delete $topicName topic: ${exception.getMessage}"),
          _ => logger.info(s"Successfully deleted $topicName topic")
        )
    }
  }

}
