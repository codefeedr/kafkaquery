package org.codefeedr.kafkaquery.transforms

import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}

class JsonToAvroSchemaTest extends AnyFunSuite with TableDrivenPropertyChecks {

  val topicName = "myTopic"

  val testData: TableFor2[String, String] =
    Table(
      ("AvroSchema", "JsonSample"),
      (
        s"""
          |{
          |    "type":"record",
          |    "name":"$topicName",
          |    "namespace":"infer",
          |    "fields":[
          |        {
          |            "name":"title",
          |            "type":"string"
          |        },
          |        {
          |            "name":"link",
          |            "type":"string"
          |        },
          |        {
          |            "name":"description",
          |            "type":"string"
          |        },
          |        {
          |            "name":"pubDate",
          |            "type":"string"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "title":"test-title",
          |    "link":"https://example.com/",
          |    "description":"test description",
          |    "pubDate":"2020-06-14T19:42:10.000Z"
          |}
          |""".stripMargin
      ),
      (
        s"""
          |{
          |    "type":"record",
          |    "name":"$topicName",
          |    "namespace":"infer",
          |    "fields":[
          |        {
          |            "name":"colors",
          |            "type":{
          |                "type":"array",
          |                "items":{
          |                "type":"record",
          |                "name":"colors",
          |                "namespace":"infer.$topicName",
          |                "fields":[
          |                    {
          |                        "name":"color",
          |                        "type":"string"
          |                    },
          |                    {
          |                        "name":"value",
          |                        "type":"string"
          |                    },
          |                    {
          |                        "name":"other",
          |                        "type":"null"
          |                    }
          |                ]
          |                }
          |            }
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "colors":[
          |        {
          |            "color":"red",
          |            "value":"#f00",
          |            "other":null
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f",
          |            "other":null
          |        },
          |        {
          |            "color":"red",
          |            "value":"#f00",
          |            "other":null
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f",
          |            "other":null
          |        }
          |    ]
          |}
          |""".stripMargin
      ),
      (
        s"""
          |{
          |    "type":"record",
          |    "name":"$topicName",
          |    "namespace":"infer",
          |    "fields":[
          |        {
          |            "name":"id",
          |            "type":"long"
          |        },
          |        {
          |            "name":"age",
          |            "type":"long"
          |        },
          |        {
          |            "name":"year",
          |            "type":"long"
          |        },
          |        {
          |            "name":"day",
          |            "type":"long"
          |        },
          |        {
          |            "name":"month",
          |            "type":"long"
          |        },
          |        {
          |            "name":"weight",
          |            "type":"double"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "id":1,
          |    "age":56,
          |    "year":1963,
          |    "day":3,
          |    "month":7,
          |    "weight":67.5
          |}
          |""".stripMargin
      ),
      (
        s"""
          |{
          |    "type":"record",
          |    "name":"$topicName",
          |    "namespace":"infer",
          |    "fields":[
          |        {
          |            "name":"id",
          |            "type":"boolean"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "id":true
          |}
          |""".stripMargin
      ),
      (
        s"""
          |{
          |    "type":"record",
          |    "name":"$topicName",
          |    "namespace":"infer",
          |    "fields":[
          |        {
          |            "name":"field_field_",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_field",
          |            "type":"string"
          |        },
          |        {
          |            "name":"_0field",
          |            "type":"string"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "field.field/":"test1",
          |    "":"test2",
          |    "/field":"test3",
          |    "0field":"test4"
          |}
          |""".stripMargin
      )
    )

  /**
    * Parameterized good weather tests for all supported types.
    */
  forAll(testData) { (avroSchema: String, jsonSample: String) =>
    assertResult(new Schema.Parser().parse(avroSchema)) {
      JsonToAvroSchema.inferSchema(jsonSample, topicName, null)
    }
  }

  val exceptionalTestData: TableFor1[String] =
    Table(
      "JsonSample",
      """
        |{
        |    "badWeather":[
        |        1,
        |        "hello"
        |    ]
        |}
        |""".stripMargin,
      """
        |{
        |    "badWeather":[
        |
        |    ]
        |}
        |""".stripMargin
    )

  /**
    * Parameterized bad weather tests.
    */
  forAll(exceptionalTestData) {jsonSample: String =>
    assertThrows[IllegalArgumentException] {
      JsonToAvroSchema.inferSchema(jsonSample, topicName, null)
    }
  }
}
