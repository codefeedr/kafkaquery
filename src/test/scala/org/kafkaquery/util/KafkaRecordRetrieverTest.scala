package org.kafkaquery.util

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

class KafkaRecordRetrieverTest extends AnyFunSuite
with BeforeAndAfter
with BeforeAndAfterAll
with EmbeddedKafka {

  val topicName = "someTopic"
  val someTopicData: List[String] = List(
    """{ "name": "title1", "retrieveDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "name": "title2", "retrieveDate": "2020-05-19T17:48:00.000Z" }""",
    """{ "name": "title3", "retrieveDate": "2020-05-19T17:48:01.000Z" }""",
    """{ "name": "title4", "retrieveDate": "2020-05-19T17:48:02.000Z" }""",
    """{ "name": "title5", "retrieveDate": "2020-05-19T17:48:03.000Z" }"""
  )

  def init(givenConfig : EmbeddedKafkaConfig) : Unit = {
    implicit val config = givenConfig
    for (i <- someTopicData.indices) {
      publishStringMessageToKafka(topicName, someTopicData(i))
    }
  }

  val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 0,
    zooKeeperPort = 0
  )



  test("Normal retrieval") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      init(config)
      val retriever = new KafkaRecordRetriever(topicName, "localhost:"+config.kafkaPort)
      for (i <- 1 to someTopicData.length) {
        val msg = retriever.getNextRecord.getOrElse("")
        assert(msg == someTopicData(someTopicData.length-i))
      }
    }
  }

  test("Retrieving more than available") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      init(config)
      val retriever = new KafkaRecordRetriever(topicName, "localhost:"+config.kafkaPort)
      for (i <- 1 to someTopicData.length) {
        val msg = retriever.getNextRecord.getOrElse("")
        assert(msg == someTopicData(someTopicData.length-i))
      }
      assert(retriever.getNextRecord.isEmpty)
    }
  }

  test("Reaching max records") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      init(config)
      val maxRecords = 3
      val retriever = new KafkaRecordRetriever(topicName, "localhost:"+config.kafkaPort, maxRecords)
      for (i <- 1 to maxRecords) {
        val msg = retriever.getNextRecord.getOrElse("")
        assert(msg == someTopicData(someTopicData.length-i))
      }
      assert(retriever.getNextRecord.isEmpty)
    }
  }


}
