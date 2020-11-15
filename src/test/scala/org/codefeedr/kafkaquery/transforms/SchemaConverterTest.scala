package org.codefeedr.kafkaquery.transforms

import java.nio.ByteBuffer

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

class SchemaConverterTest extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Nested(someVal: Int, someOtherVal: String)

  case class AllSupportedTypes(someString: String, someFloat: Float, someDouble: Double, someInt: Int,
                               someBoolean: Boolean, someLong: Long, someOptional: Option[String], someByte: ByteBuffer,
                               someMap: Map[String, Int], someArray: Array[Int], someList: List[Long], someNested: Nested,
                               someNestedList: List[Nested])

  val schema: Schema = AvroSchema[AllSupportedTypes]

  val testData: TableFor2[String, String] =
    Table(
      ("FieldName", "expectedType"),

      ("someString", "STRING"),
      ("someFloat", "FLOAT"),
      ("someDouble", "DOUBLE"),
      ("someInt", "INTEGER"),
      ("someBoolean", "BOOLEAN"),
      ("someLong", "BIGINT"),
      ("someOptional", "STRING"),
      ("someByte", "BYTES"),
      ("someMap", "MAP<STRING, INTEGER>"),
      ("someArray", "ARRAY<INTEGER>"),
      ("someList", "ARRAY<BIGINT>"),
      ("someNested", "ROW<`someVal` INTEGER, `someOtherVal` STRING>"),
      ("someNestedList", "ARRAY<ROW<`someVal` INTEGER, `someOtherVal` STRING>>")
    )

  /**
   * Parameterized good weather tests for all supported types.
   */
  forAll(testData) { (name: String, t: String) =>
    assertResult(("`" + name + "`", t)) {
      val res = getNestedSchema(name, schema.getField(name).schema())
      (res._1, res._2.toString)
    }
  }

  /**
    * Test unsupported types.
    */
  assertThrows[RuntimeException] {

    val schemaString =
      """
        |{ "type" : "enum",
        |  "name" : "Colors",
        |  "symbols" : ["WHITE", "BLACK"]}
        |""".stripMargin
    getNestedSchema("does not matter", new Schema.Parser().parse(schemaString))
  }

}
