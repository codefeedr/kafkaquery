package org.codefeedr.kafkaquery.transforms

import org.apache.avro.Schema

import scala.collection.JavaConverters._

object QuerySetup {

  /** Extract the table names from the query and return the ones that are supported.
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

  /** Returns the Flink DDL for creating the specified temporary table.
    * @param name name of the table/Kafka topic
    * @param tableFields DDL StringBuilder of the table fields
    * @param kafkaAddr Kafka server address
    * @param checkLatest record retrieval strategy (from LATEST or EARLIEST)
    * @return temporary table DDL string
    */
  def getTableCreationCommand(
      name: String,
      tableFields: java.lang.StringBuilder,
      kafkaAddr: String,
      checkLatest: Boolean
  ): String = {
    "CREATE TEMPORARY TABLE `" + name + "` (" + tableFields + ") " +
      "WITH (" +
      "'connector.type' = 'kafka', " +
      "'connector.version' = 'universal', " +
      "'connector.topic' = '" + name + "', " +
      "'connector.properties.bootstrap.servers' = '" + kafkaAddr + "', " +
      "'connector.startup-mode' = '" +
      (if (checkLatest) "latest-offset" else "earliest-offset") +
      "', " +
      "'connector.properties.default.api.timeout.ms' = '5000', " + //Todo create option for setting this value
      "'format.type' = 'json', " +
      "'format.fail-on-missing-field' = 'false'" +
      ")"
  }

  /** Generate a Flink table schema from an Avro Schema.
    *
    * @param avroSchema  the Avro Schema
    * @return a java.lang.StringBuilder with the DDL description of the schema
    */
  def generateTableSchema(
      avroSchema: org.apache.avro.Schema,
      fieldGetter: (String, Schema) => (String, java.lang.StringBuilder)
  ): java.lang.StringBuilder = {
    val res = new java.lang.StringBuilder()

    val rowtimeEnabled = !"false".equals(avroSchema.getProp("rowtime"))

    var rowtimeFound = false

    for (field <- avroSchema.getFields.asScala) {
      val fieldInfo = fieldGetter(field.name(), field.schema())

      if (
        rowtimeEnabled && !rowtimeFound && field.getProp("rowtime") == "true"
      ) {
        rowtimeFound = true
        res.append(
          fieldInfo._1 + " TIMESTAMP(3), WATERMARK FOR " + fieldInfo._1 +
            " AS " + fieldInfo._1 + " - INTERVAL '0.001' SECOND, "
        )
      } else {
        res.append(fieldInfo._1 + " " + fieldInfo._2 + ", ")
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
