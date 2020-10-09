package org.codefeedr.kafkaquery.transforms

import org.apache.avro.Schema.Type

import scala.collection.JavaConverters._

object SchemaConverter {

  /**
    * Getter for the nested Avro schema.
    *
    * @param name name of the schema
    * @param schema schema of current level
    * @return schema name with the corresponding Flink type name as a tuple
    */
  def getNestedSchema(
      name: String,
      schema: org.apache.avro.Schema
  ): (String, java.lang.StringBuilder) = {

    val escapedNamed = '`' + name + "`"

    schema.getType match {
      case Type.NULL    => (escapedNamed, new java.lang.StringBuilder("NULL"))
      case Type.STRING  => (escapedNamed, new java.lang.StringBuilder("STRING"))
      case Type.FLOAT   => (escapedNamed, new java.lang.StringBuilder("FLOAT"))
      case Type.DOUBLE  => (escapedNamed, new java.lang.StringBuilder("DOUBLE"))
      case Type.INT     => (escapedNamed, new java.lang.StringBuilder("INTEGER"))
      case Type.BOOLEAN => (escapedNamed, new java.lang.StringBuilder("BOOLEAN"))
      case Type.LONG    => (escapedNamed, new java.lang.StringBuilder("BIGINT"))
      case Type.BYTES   => (escapedNamed, new java.lang.StringBuilder("BYTES"))

      case Type.UNION =>
        val foundType = schema.getTypes.asScala
          .map(getNestedSchema(name, _)._2)
          .find(x => x.toString != "NULL")
        (
          escapedNamed,
          if (foundType.isDefined) foundType.get
          else new java.lang.StringBuilder("NULL")
        )

      // The key for an Avro map must be a string. Avro maps supports only one attribute: values.
      case Type.MAP =>
        (
          escapedNamed,
          new java.lang.StringBuilder("MAP<STRING, ")
            .append(getNestedSchema(name, schema.getValueType)._2)
            .append(">")
        )

      case org.apache.avro.Schema.Type.ARRAY =>
        (
          escapedNamed,
          new java.lang.StringBuilder("ARRAY<")
            .append(getNestedSchema(name, schema.getElementType)._2)
            .append(">")
        )

      case org.apache.avro.Schema.Type.RECORD =>
        val fieldIterator = schema.getFields.asScala
          .map(x => getNestedSchema(x.name(), x.schema()))
          .iterator

        val res = new java.lang.StringBuilder("ROW<")

        if (fieldIterator.hasNext) {
          val nextVal = fieldIterator.next()
          res.append(nextVal._1 + " " + nextVal._2)
        }

        while (fieldIterator.hasNext) {
          val nextVal = fieldIterator.next()
          res.append(", " + nextVal._1 + " " + nextVal._2)
        }

        res.append(">")

        (escapedNamed, res)

      case _ => throw new RuntimeException("Unsupported type.")
    }
  }

}
