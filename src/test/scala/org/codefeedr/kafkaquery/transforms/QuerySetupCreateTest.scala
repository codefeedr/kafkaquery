package org.codefeedr.kafkaquery.transforms

import org.apache.avro.Schema
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

class QuerySetupCreateTest extends AnyFunSuite with BeforeAndAfter with TableDrivenPropertyChecks {

  test("Topics should correctly be extracted from query") {
    val res = QuerySetup.extractTopics("SELECT * from t1, t2, t3", List("t2", "t3", "t4"))
    assert(res == List("t2", "t3"))
  }

  val testDatagetTableCreationCommand: TableFor2[Boolean, String] =
    Table(
      ("checkLatest", "offsetText"),

      (false, "earliest-offset"),
      (true, "latest-offset")
    )
  forAll(testDatagetTableCreationCommand) { (checkLatest: Boolean, offsetText: String) =>
    val tableName = "t1"
    val tableFields = "(f1 INT, f2 STRING)"
    val kafkaAddr = "localhost:9092"

    assertResult(
      s"CREATE TEMPORARY TABLE t1 ((f1 INT, f2 STRING)) WITH ('connector.type' = 'kafka', " +
        s"'connector.version' = 'universal', 'connector.topic' = 't1', 'connector.properties.bootstrap.servers' " +
        s"= 'localhost:9092', 'connector.startup-mode' = '$offsetText', 'format.type' = 'json', " +
        s"'format.fail-on-missing-field' = 'false')"
    )(
      QuerySetup.getTableCreationCommand(tableName, new java.lang.StringBuilder(tableFields), kafkaAddr,
        checkLatest = checkLatest)
    )
  }

  val testDataGenerateTableSchema: TableFor2[String, String] =
    Table(
      ("schemaStr", "tableDesc"),

      (
        """
          |{
          |     "type": "record",
          |     "name": "t1",
          |     "fields": [
          |       { "name": "f1", "type": "string" },
          |       { "name": "f2", "type": "int" }
          |     ]
          |}
          |""".stripMargin,
        "f1 STRING, f2 INTEGER, kafka_time AS PROCTIME()"
      ),
      (
        """
          |{
          |     "type": "record",
          |     "name": "t1",
          |     "rowtime": "false",
          |     "fields": [
          |       { "name": "f1", "type": "string" },
          |       { "name": "f2", "type": "int" }
          |     ]
          |}
          |""".stripMargin,
        "f1 STRING, f2 INTEGER"
      ),
      (
        """
          |{
          |     "type": "record",
          |     "name": "t1",
          |     "fields": [
          |       { "name": "f1", "type": "string" },
          |       { "name": "f2", "type": "int", "rowtime": "true" }
          |     ]
          |}
          |""".stripMargin,
        "f1 STRING, f2 TIMESTAMP(3), WATERMARK FOR f2 AS f2 - INTERVAL '0.001' SECOND"
      )
    )
  forAll(testDataGenerateTableSchema) { (schemaStr: String, tableDesc: String) =>
    assertResult(
      tableDesc
    )(
      QuerySetup.generateTableSchema(new Schema.Parser().parse(schemaStr)).toString
    )
  }

}
