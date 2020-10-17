package org.codefeedr.kafkaquery.transforms

import org.apache.avro.Schema
import org.codefeedr.kafkaquery.util.KafkaRecordRetriever
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}

class JsonToAvroSchemaTest extends AnyFunSuite with TableDrivenPropertyChecks with MockitoSugar {

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
          |            "type":"long"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "title":"test-title",
          |    "link":"https://example.com/",
          |    "description":"test description",
          |    "pubDate": 1
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
          |                        "type":"long"
          |                    }
          |                ]
          |                }
          |            }
          |        },
          |        {
          |            "name":"pubDate",
          |            "type":"long"
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
          |            "other":42
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f",
          |            "other":43
          |        },
          |        {
          |            "color":"red",
          |            "value":"#f00",
          |            "other":44
          |        },
          |        {
          |            "color":"magenta",
          |            "value":"#f0f",
          |            "other":43
          |        }
          |    ],
          |    "pubDate": 1
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
          |        },
          |        {
          |            "name":"pubDate",
          |            "type":"long"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "id":true,
          |    "pubDate": 1
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
          |        },
          |        {
          |            "name":"pubDate",
          |            "type":"long"
          |        }
          |    ]
          |}
          |""".stripMargin,
        """
          |{
          |    "field.field/":"test1",
          |    "":"test2",
          |    "/field":"test3",
          |    "0field":"test4",
          |    "pubDate":1
          |}
          |""".stripMargin
      )
    )

  /**
    * Parameterized good weather tests for all supported types.
    */
  forAll(testData) { (avroSchema: String, jsonSample: String) =>
    assertResult(new Schema.Parser().parse(avroSchema)) {
      val recordRetrieverMock = mock[KafkaRecordRetriever]
      doReturn(Option(jsonSample)).when(recordRetrieverMock).getNextRecord

      JsonToAvroSchema.inferSchema(topicName, recordRetrieverMock)
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
        |""".stripMargin
      /*
      TODO Adjust this test case to new way of handling empty arrays
      ,"""
        |{
        |    "badWeather":[
        |
        |    ]
        |}
        |""".stripMargin*/
    )

  /**
    * Parameterized bad weather tests.
    */
  forAll(exceptionalTestData) {jsonSample: String =>
    assertThrows[IllegalArgumentException] {
      val recordRetrieverMock = mock[KafkaRecordRetriever]
      doReturn(Option(jsonSample)).when(recordRetrieverMock).getNextRecord

      JsonToAvroSchema.inferSchema(topicName, recordRetrieverMock)
    }
  }
}
