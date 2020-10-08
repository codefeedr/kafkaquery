package org.codefeedr.kafkaquery.transforms

import java.time.Duration
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.codefeedr.kafkaquery.util.KafkaRecordRetriever

import scala.collection.JavaConverters._

object JsonToAvroSchema {

  /**
    * Infers an Avro schema from a given JSON object.
    * @param json json data to infer schema from
    * @param name name of the data source
    * @param retriever a record retriever
    * @return inferred Avro Schema
    */
  def inferSchema(
      json: String,
      name: String,
      retriever: KafkaRecordRetriever
  ): Schema = {
    inferSchema(
      new ObjectMapper().readTree(json),
      SchemaBuilder.builder(),
      name,
      retriever
    )
  }

  def inferSchema[T](
      node: JsonNode,
      schema: TypeBuilder[T],
      name: String,
      retriever: KafkaRecordRetriever,
      namespace: String = "infer"
  ): T =
    node.getNodeType match {
      case JsonNodeType.ARRAY =>
        val it = node.iterator()

        if (!it.hasNext)
          return findArrayType(name, retriever, schema)

        val nodeName = validName(name)

        val arrayElemSchema =
          inferSchema(
            it.next(),
            SchemaBuilder.builder(),
            nodeName,
            retriever,
            namespace
          )

        it.forEachRemaining(x =>
          if (
            !arrayElemSchema.equals(
              inferSchema(
                x,
                SchemaBuilder.builder(),
                nodeName,
                retriever,
                namespace
              )
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
                retriever,
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

  private def findArrayType[T](
      arrayName: String,
      retriever: KafkaRecordRetriever,
      schema: TypeBuilder[T]
  ): T = {
    val recordString = retriever.getNextRecord
    val nextRecordNode = new ObjectMapper().readTree(recordString)
    inferSchema(
      nextRecordNode.findPath(arrayName),
      schema,
      arrayName,
      retriever
    )
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
