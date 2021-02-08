package org.kafkaquery.transforms

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.Schema.Type
import org.apache.avro.SchemaBuilder.TypeBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.kafkaquery.util.{KafkaRecordRetriever, UserInputRetriever}

import scala.collection.mutable.ListBuffer

object JsonToAvroSchema {

  /** Infers an Avro schema from a given JSON object.
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
        val fieldIt = node.fields()

        if (fieldIt.hasNext) {
          val fieldList: ListBuffer[(String, Schema)] =
            new ListBuffer[(String, Schema)]
          var allFieldsSameType = true
          val firstField = fieldIt.next()
          val firstFieldName = validName(firstField.getKey)
          fieldList.append(
            (
              firstFieldName,
              inferSchema(
                firstField.getValue,
                SchemaBuilder.builder(),
                firstFieldName,
                retriever,
                namespace + '.' + nodeName
              )
            )
          )

          fieldIt.forEachRemaining(x => {
            val fieldName = validName(x.getKey)
            val fieldSchema = inferSchema(
              x.getValue,
              SchemaBuilder.builder(),
              fieldName,
              retriever,
              namespace + '.' + nodeName
            )
            fieldList.append((fieldName, fieldSchema))
            allFieldsSameType = allFieldsSameType && areSchemaTypesEqual(
              fieldList.head._2,
              fieldSchema
            )
          })

          if (allFieldsSameType) {
            println("Should this be a map (m) or an object (o)?")
            println(node.toPrettyString)
            val res = UserInputRetriever.readAllowedChar(List('m', 'o'))

            // TODO write more neatly
            if (res == 'm') {
              return schema.map().values(fieldList.head._2)
            }
          }

          val newSchema = schema.record(nodeName).namespace(namespace).fields()
          fieldList.foreach(x => newSchema.name(x._1).`type`(x._2).noDefault())
          newSchema.endRecord()

        } else {
          // TODO either ask user or do as for arrays
          println("erm")
          schema.record("temp").fields().endRecord()
        }

      case JsonNodeType.BOOLEAN => schema.booleanType()

      case JsonNodeType.STRING | JsonNodeType.BINARY => schema.stringType()

      case JsonNodeType.NUMBER =>
        if (node.isIntegralNumber) schema.longType() else schema.doubleType()

      case JsonNodeType.NULL | JsonNodeType.MISSING => schema.stringType()
    }

  private def areSchemaTypesEqual(one: Schema, two: Schema): Boolean = {
    val schemaTypes = (one.getType, two.getType)
    schemaTypes match {
      case (Type.RECORD, Type.RECORD) =>
        if (one.getFields.size() != two.getFields.size())
          return false

        for (i <- 0 until one.getFields.size()) {

          if (
            one.getFields.get(i).name() != two.getFields.get(i).name() ||
            !areSchemaTypesEqual(
              one.getFields.get(i).schema(),
              two.getFields.get(i).schema()
            )
          )
            return false
        }
        true
      case (Type.ARRAY, Type.ARRAY) =>
        areSchemaTypesEqual(one.getElementType, two.getElementType)
      case (Type.MAP, Type.MAP) =>
        areSchemaTypesEqual(one.getValueType, two.getValueType)
      case (x, y) if x == y => true
      case _                => false
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

  /** Modified name to ensure it adheres to Avro requirements by replacing illegal characters with '_'.
    * @param name original name of the field
    * @return valid name accepted by Avro
    */
  private def validName(name: String): String = {
    val tempName = name.replaceAll("\\W", "_")
    if (
      name.nonEmpty &&
      (tempName.charAt(0).isLetter || tempName.charAt(0) == '_')
    )
      return tempName
    '_' + tempName
  }
}
