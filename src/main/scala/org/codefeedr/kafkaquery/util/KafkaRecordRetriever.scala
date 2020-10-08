package org.codefeedr.kafkaquery.util

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/** *
  * Retrieves records from a specified Kafka topic in reverse order.
  * @param topicName name of the topic to retrieve records from
  * @param kafkaAddress address of Kafka instance to connect to
  */
class KafkaRecordRetriever(topicName: String, kafkaAddress: String) {

  private val props = new Properties()
  props.put("bootstrap.servers", kafkaAddress)
  props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put("max.poll.records", "1")

  private val kafkaConsumer = new KafkaConsumer[String, String](props)

  private val partitions = kafkaConsumer
    .partitionsFor(topicName)
    .asScala
    .map(x => new TopicPartition(x.topic(), x.partition()))
    .asJava

  kafkaConsumer.assign(partitions)

  private val partEndOffsetMap = kafkaConsumer.endOffsets(partitions).asScala
  private val partBegOffsetMap =
    kafkaConsumer.beginningOffsets(partitions).asScala

  /**
    * Retrieves the next record from the inverse order.
    * @return the next record that has not been retrieved yet
    */
  def getNextRecord: String = {

    val maxOffsetKey =
      partEndOffsetMap.maxBy(x => x._2 - partBegOffsetMap(x._1))._1

    partEndOffsetMap(maxOffsetKey) -= 1

    if (partEndOffsetMap(maxOffsetKey) < partBegOffsetMap(maxOffsetKey))
      throw new IllegalArgumentException(
        "Topic does not contain enough messages to be inferred properly."
      )

    kafkaConsumer.seek(maxOffsetKey, partEndOffsetMap(maxOffsetKey))

    val records: ConsumerRecords[String, String] =
      kafkaConsumer.poll(Duration.ofMillis(2000))

    records.iterator().next().value()
  }

}
