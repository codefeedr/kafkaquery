package org.codefeedr.kafkaquery.transforms

import java.nio.ByteBuffer

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.io.FastReaderBuilder.RecordReader.Stage
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

class SchemaConverterTest extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Nested(someValue: Int)

  case class someStage(someStage: Stage)

  case class AllSupportedTypes(someString: String, someFloat: Float, someDouble: Double, someInt: Int,
                               someBoolean: Boolean, someLong: Long, someOptional: Option[String], someByte: ByteBuffer,
                               someMap: Map[String, Int], someArray: Array[Int], someList: List[Long], someNested: Nested,
                               someNestedList: List[Nested], someNull: Null)

  val schema: Schema = {
    implicit val nullSchemaFor: AnyRef with SchemaFor[Null] = SchemaFor[Null](Schema.create(Schema.Type.NULL))
    AvroSchema[AllSupportedTypes]
  }

  val testData: TableFor2[String, String] =
    Table(
      ("expectedType", "FieldName"),
      ("STRING", "someString"),
      ("FLOAT", "someFloat"),
      ("DOUBLE", "someDouble"),
      ("INTEGER", "someInt"),
      ("BOOLEAN", "someBoolean"),
      ("BIGINT", "someLong"),
      ("STRING", "someOptional"),
      ("BYTES", "someByte"),
      ("MAP<STRING, INTEGER>", "someMap"),
      ("ARRAY<INTEGER>", "someArray"),
      ("ARRAY<BIGINT>", "someList"),
      ("ROW<someValue INTEGER>", "someNested"),
      ("ARRAY<ROW<someValue INTEGER>>", "someNestedList"),
      ("NULL", "someNull")
    )

  /**
   * Parameterized good weather tests for all supported types.
   */
  forAll(testData) { (t: String, name: String) =>
    assertResult((name, t)) {
      val res = getNestedSchema(name, schema.getField(name).schema())
      (res._1, res._2.toString)
    }
  }
  /**
   * Unsupported type.
   */
  assertThrows[RuntimeException] {
    getNestedSchema("does not matter", AvroSchema[someStage])
  }

}
