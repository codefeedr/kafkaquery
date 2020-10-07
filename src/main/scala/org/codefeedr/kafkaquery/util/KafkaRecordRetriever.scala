package org.codefeedr.kafkaquery.util

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class KafkaRecordRetriever(topicName: String, kafkaAddress: String) {

  var n = 1

  val props = new Properties()
  props.put("bootstrap.servers", kafkaAddress)
  props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val kafkaConsumer = new KafkaConsumer[String, String](props)

  val partitions: Seq[TopicPartition] = kafkaConsumer
    .partitionsFor(topicName)
    .asScala
    .map(x => new TopicPartition(x.topic(), x.partition()))

  kafkaConsumer.assign(partitions.asJava)

  def getNextRecord: String = {

    kafkaConsumer.seekToEnd(List().asJava)

    val latestPartAndPos: (TopicPartition, Long) =
      partitions.map(x => (x, kafkaConsumer.position(x))).maxBy(_._2)

    if (latestPartAndPos._2 < n)
      throw new IllegalArgumentException(
        "Topic does not contain enough messages to be inferred properly."
      )

    kafkaConsumer.seek(latestPartAndPos._1, latestPartAndPos._2 - n)

    val records: ConsumerRecords[String, String] =
      kafkaConsumer.poll(Duration.ofMillis(100))

    n += 1

    records.iterator().next().value()
  }

}
