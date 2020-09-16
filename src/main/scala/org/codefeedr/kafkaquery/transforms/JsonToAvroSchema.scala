package org.codefeedr.kafkaquery.transforms

import java.time.Duration
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._

object JsonToAvroSchema {

  /**
    * Infers an Avro schema from a given JSON object.
    * @param json json data to infer schema from
    * @param name name of the data source
    * @return inferred Avro Schema
    */
  def inferSchema(json: String, name: String): Schema = {
    inferSchema(
      new ObjectMapper().readTree(json),
      SchemaBuilder.builder(),
      name
    )
  }

  private def inferSchema[T](
      node: JsonNode,
      schema: TypeBuilder[T],
      name: String,
      namespace: String = "infer"
  ): T =
    node.getNodeType match {
      case JsonNodeType.ARRAY =>
        val it = node.iterator()

        if (!it.hasNext)
          throw new IllegalArgumentException(
            "Could not infer schema of empty array."
          )

        val nodeName = validName(name)

        val arrayElemSchema =
          inferSchema(it.next(), SchemaBuilder.builder(), nodeName, namespace)

        it.forEachRemaining(x =>
          if (
            !arrayElemSchema.equals(
              inferSchema(x, SchemaBuilder.builder(), nodeName, namespace)
            )
          )
            throw new IllegalArgumentException(
              "Array contains elements of different types."
            )
        )

        schema.array().items(arrayElemSchema)

      case JsonNodeType.OBJECT | JsonNodeType.POJO =>
        val nodeName = validName(name)

        val newSchema = schema.record(nodeName).namespace(namespace).fields()
        node.fields.forEachRemaining(x => {
          val fieldName = validName(x.getKey)
          newSchema
            .name(fieldName)
            .`type`(
              inferSchema(
                x.getValue,
                SchemaBuilder.builder(),
                fieldName,
                namespace + '.' + nodeName
              )
            )
            .noDefault()
        })
        newSchema.endRecord()

      case JsonNodeType.BOOLEAN => schema.booleanType()

      case JsonNodeType.STRING | JsonNodeType.BINARY => schema.stringType()

      case JsonNodeType.NUMBER =>
        if (node.isIntegralNumber) schema.longType() else schema.doubleType()

      case JsonNodeType.NULL | JsonNodeType.MISSING => schema.nullType()
    }

  /**
    * Gets the latest record from the specified topic.
    * @param topicName name of the topic to fetch record from
    * @param kafkaAddress address of Kafka instance
    * @return string with the value of the last record
    */
  def retrieveLatestRecordFromTopic(
      topicName: String,
      kafkaAddress: String
  ): String = {
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

    val partitions = kafkaConsumer
      .partitionsFor(topicName)
      .asScala
      .map(x => new TopicPartition(x.topic(), x.partition()))

    kafkaConsumer.assign(partitions.asJava)

    kafkaConsumer.seekToEnd(List().asJava)

    val latestPartAndPos =
      partitions.map(x => (x, kafkaConsumer.position(x))).maxBy(_._2)
    kafkaConsumer.seek(latestPartAndPos._1, latestPartAndPos._2 - 1)

    val records = kafkaConsumer.poll(Duration.ofMillis(100))

    records.iterator().next().value()
  }

  /**
    * Modified name to ensure it adheres to Avro requirements by replacing illegal characters with '_'.
    * @param name original name of the field
    * @return valid name accepted by Avro
    */
  private def validName(name: String): String = {
    val tempName = name.replaceAll("\\W", "_")
    if (
      !name.isBlank &&
      (tempName.charAt(0).isLetter || tempName.charAt(0) == '_')
    )
      return tempName
    '_' + tempName
  }
}
