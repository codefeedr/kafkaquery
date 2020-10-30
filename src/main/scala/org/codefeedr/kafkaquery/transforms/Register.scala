package org.codefeedr.kafkaquery.transforms

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.codefeedr.kafkaquery.util.ZookeeperSchemaExposer

import scala.collection.JavaConverters._

trait Register {

  /**
    * Register and apply the tables of the given query
    * with the ZookeeperAddress and KafkaAddress.
    *
    * @param query            the input query
    * @param zookeeperAddress address of Zookeeper specified as an environment variable. (Default address: 'localhost:2181')
    * @param kafkaAddress     address of Kafka specified as an environment variable. (Default address: 'localhost:9092')
    * @param startLatest      flag to specify starting Kafka from-latest if true (from-earliest used if false)
    * @return tuple with the DataStream of rows and the streamExecutionEnvironment
    */
  def registerAndApply(
      query: String,
      zookeeperAddress: String,
      kafkaAddress: String,
      startLatest: Boolean
  ): (DataStream[Row], StreamExecutionEnvironment) = {

    val zk = new ZookeeperSchemaExposer(zookeeperAddress)

    val supportedFormats = zk.getAllChildren
    println("Supported Plugins: " + supportedFormats)

    val requestedTopics = extractTopics(query, supportedFormats)

    println("Requested: " + requestedTopics)

    val fsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val fsEnv =
      StreamExecutionEnvironment.getExecutionEnvironment

    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    fsEnv.getConfig.enableObjectReuse()

    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)

    for (topicName <- requestedTopics) {
      val result = zk.get(topicName)

      if (result.isDefined) {
        val ddlString =
          "CREATE TEMPORARY TABLE `" + topicName + "` (" +
            generateFlinkTableSchema(result.get) +
            ") WITH (" +
            "'connector.type' = 'kafka', " +
            "'connector.version' = 'universal', " +
            "'connector.topic' = '" + topicName + "', " +
            "'connector.properties.bootstrap.servers' = '" + kafkaAddress + "', " +
            "'connector.startup-mode' = '" +
            (if (startLatest) "latest-offset" else "earliest-offset") +
            "', " +
            "'format.type' = 'json', " +
            "'format.fail-on-missing-field' = 'false'" +
            ")"

        fsTableEnv.executeSql(ddlString)
      }
    }

    (fsTableEnv.sqlQuery(query).toRetractStream[Row].map(_._2), fsEnv)
  }

  /**
    * Extract the table names from the query and return the ones that are supported.
    *
    * @param query            sql query to be matched
    * @param supportedPlugins list of supported plugins from zookeeper
    * @return the subset of supported plugins
    */
  def extractTopics(
      query: String,
      supportedPlugins: List[String]
  ): List[String] = {
    supportedPlugins.intersect(query.split("\\s+|,|;|\\(|\\)|`"))
  }

  /**
    * Generate a Flink table schema from an Avro Schema.
    *
    * @param avroSchema  the Avro Schema
    * @return a java.lang.StringBuilder with the DDL description of the schema
    */
  private def generateFlinkTableSchema(
      avroSchema: org.apache.avro.Schema
  ): java.lang.StringBuilder = {
    val res = new java.lang.StringBuilder()

    val rowtimeEnabled = !"false".equals(avroSchema.getProp("rowtime"))

    var rowtimeFound = false

    for (field <- avroSchema.getFields.asScala) {
      val fieldInfo = getNestedSchema(field.name(), field.schema())

      if (
        rowtimeEnabled && !rowtimeFound && "true".equals(
          field.getProp("rowtime")
        )
      ) {
        rowtimeFound = true

        res.append(fieldInfo._1)
        res.append(" TIMESTAMP(3), WATERMARK FOR ")
        res.append(fieldInfo._1)
        res.append(" AS ")
        res.append(fieldInfo._1)
        res.append(" - INTERVAL '0.001' SECOND, ")
      } else {
        res.append(fieldInfo._1)
        res.append(" ")
        res.append(fieldInfo._2)
        res.append(", ")
      }
    }

    if (rowtimeEnabled && !rowtimeFound) {
      res.append("`kafka_time` AS PROCTIME(), ")
    }

    if (res.length() - 2 == res.lastIndexOf(", "))
      res.delete(res.length() - 2, res.length())

    res
  }

}
