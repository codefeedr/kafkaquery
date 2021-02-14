package org.kafkaquery.transforms

import org.apache.avro.Schema
import org.apache.avro.Schema.Type

object SimpleSchemaGenerator {

  /** Produces and returns a simplified version of the specified Avro schema.
    * @param schema the schema to be simplified
    * @return A simplified schema representation
    */
  def getSimpleSchema(schema: Schema): String = {
    getSimpleSchema(schema, 0)
  }
  private def getSimpleSchema(
      schema: Schema,
      depth: Int,
      fieldName: String = null
  ): String = {
    var actualSchema = schema
    val builder = new StringBuilder("")
    for (_ <- 1 to depth) {
      builder.append("     ")
    }

    var name = ""
    if (fieldName == null) {
      name = actualSchema.getName
    } else {
      name = fieldName
    }

    val schemaTypeText = actualSchema.getType match {
      case Type.ARRAY =>
        Type.ARRAY + "<" + actualSchema.getElementType.getType + ">"
      case Type.MAP => Type.MAP + "<" + actualSchema.getValueType.getType + ">"
      case Type.UNION =>
        val tmp = actualSchema.getTypes.get(1).getType + " (NULLABLE)"
        actualSchema = actualSchema.getTypes.get(1)
        tmp
      case _ => actualSchema.getType
    }

    builder.append(name + " : " + schemaTypeText + "\n")

    if (actualSchema.getType.equals(Type.RECORD)) {
      actualSchema.getFields.forEach(field => {
        builder.append(
          getSimpleSchema(field.schema(), depth + 1, field.name())
        )
      })
    }

    builder.toString()
  }

}
