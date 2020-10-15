package org.codefeedr.kafkaquery.transforms

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.Schema.Type
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.codefeedr.kafkaquery.util.KafkaRecordRetriever

import scala.collection.mutable.ListBuffer

object JsonToAvroSchema {

  /**
    * Infers an Avro schema from a given JSON object.
    * @param topicName name of the Kafka data source
    * @param recordRetriever retriever of Kafka records in inverse order of addition
    * @return inferred Avro Schema
    */
  def inferSchema(
      topicName: String,
      recordRetriever: KafkaRecordRetriever
  ): Schema = {

    inferSchema(
      new ObjectMapper().readTree(
        recordRetriever.getNextRecord.getOrElse(
          throw new IllegalArgumentException(
            "Can not infer schema of empty topic."
          )
        )
      ),
      SchemaBuilder.builder(),
      topicName,
      recordRetriever
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
        if(node.fields.hasNext) {

          var fieldList : ListBuffer[(String, Schema)] = new ListBuffer[(String, Schema)]
          val allFieldsSameType = true
          val firstField = node.fields.next()
          val fieldName = validName(firstField.getKey)
          fieldList :+ (firstField, fieldName)

          val firstSchema = inferSchema(
            firstField.getValue,
            SchemaBuilder.builder(),
            fieldName,
            retriever,
            namespace + '.' + nodeName
          )

          node.fields.forEachRemaining(x => {
            val fieldName = validName(x.getKey)
            val field = inferSchema(
              x.getValue,
              SchemaBuilder.builder(),
              fieldName,
              retriever,
              namespace + '.' + nodeName
            )
            fieldList :+ (firstField, fieldName)
            allFieldsSameType = allFieldsSameType && areSchemaTypesEqual(firstField, field)
          })

          newSchema.name(fieldName)
            .`type`(
              //go through all
            )
            .noDefault()
        }

        newSchema.endRecord()

      case JsonNodeType.BOOLEAN => schema.booleanType()

      case JsonNodeType.STRING | JsonNodeType.BINARY => schema.stringType()

      case JsonNodeType.NUMBER =>
        if (node.isIntegralNumber) schema.longType() else schema.doubleType()

      case JsonNodeType.NULL | JsonNodeType.MISSING => schema.stringType()
    }

  private def areSchemaTypesEqual(one: Schema, two: Schema) : Boolean = {
    val schemaTypes = (one.getType, two.getType)
    schemaTypes match {
      case (Type.RECORD, Type.RECORD) => {
        if(one.getFields.size() != two.getFields.size())
          return false

        for(i <- 0 until one.getFields.size()) {

          if (one.getFields.get(i).name() != two.getFields.get(i).name() ||
            !areSchemaTypesEqual(one.getFields.get(i).schema(), two.getFields.get(i).schema()))
            return false
        }
        true
      }
      case (Type.ARRAY, Type.ARRAY) => areSchemaTypesEqual(one.getElementType, two.getElementType)
      case (Type.MAP, Type.MAP) => areSchemaTypesEqual(one.getValueType, two.getValueType)
      case (x, y) if x == y => true
      case _ => false
    }

  }

  private def findArrayType[T](
      arrayName: String,
      retriever: KafkaRecordRetriever,
      schema: TypeBuilder[T]
  ): T = {
    val recordString = retriever.getNextRecord

    if (recordString.isEmpty)
      return schema.stringType()

    val nextRecordNode = new ObjectMapper().readTree(recordString.get)

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
