package org.codefeedr.kafkaquery.parsers

import org.codefeedr.kafkaquery.parsers.Configurations.Mode.Mode

object Configurations {

  /** Config which contains all the copied the arguments from the CLI
    *
    * @param mode        an enum of the command being used
    * @param queryConfig query config containing the arguments related to the query
    * @param topicName   topic name
    * @param kafkaAddress the Kafka address (default localhost:9092 or environment variable KAFKA_ADDR if present)
    * @param zookeeperAddress the ZooKeeper address (default localhost:2181 or environment variable ZK_ADDR if present)
    */
  case class Config(
      mode: Mode = null,
      queryConfig: QueryConfig = QueryConfig(),
      topicName: String = "",
      avroSchema: String = "",
      kafkaAddress: String = sys.env.getOrElse("KAFKA_ADDR", "localhost:9092"),
      zookeeperAddress: String = sys.env.getOrElse("ZK_ADDR", "localhost:2181")
  )

  /** Mode decides which config arguments to choose
    */
  object Mode extends Enumeration {
    type Mode = Value
    val Query, Topic, Topics, Schema, Infer = Value
  }

  /** Query config which contains copied arguments from the CLI
    *
    * @param query       query string
    * @param outTopic    output Kafka topic
    * @param port        socket port number
    * @param timeout     timeout before query prints results
    * @param checkLatest check if the query output should be printed from latest
    * @param ignoreParseErr ignore JSON parse errors during deserialization
    * @param pack        generate JAR which can be deployed to Flink cluster
    */
  case class QueryConfig(
      query: String = "",
      outTopic: String = "",
      port: Int = -1,
      timeout: Int = -1,
      timeoutFunc: () => Unit = () => System.exit(0),
      checkEarliest: Boolean = false,
      checkLatest: Boolean = false,
      ignoreParseErr: Boolean = true,
      pack: Boolean = false
  )
}
