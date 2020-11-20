package org.codefeedr.kafkaquery.transforms

import java.lang

import org.apache.avro.Schema
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

class QuerySetupTest extends AnyFunSuite with BeforeAndAfter with TableDrivenPropertyChecks {

  test("Topics should correctly be extracted from query") {
    val res = QuerySetup.extractTopics("SELECT * from t1, t2, t3", List("t2", "t3", "t4"))
    assert(res == List("t2", "t3"))
  }

  val testDataGetTableCreationCommand: TableFor2[Boolean, String] =
    Table(
      ("checkLatest", "offsetText"),

      (false, "earliest-offset"),
      (true, "latest-offset")
    )
  forAll(testDataGetTableCreationCommand) { (checkLatest: Boolean, offsetText: String) =>
    val tableName = "t1"
    val tableFields = "(f1 INT, f2 STRING)"
    val kafkaAddr = "localhost:9092"

    assertResult(
      s"CREATE TEMPORARY TABLE `t1` ((f1 INT, f2 STRING)) WITH ('connector.type' = 'kafka', " +
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
        "field field type, field field type, `kafka_time` AS PROCTIME()"
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
        "field field type, field field type"
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
        "field field type, field TIMESTAMP(3), WATERMARK FOR field AS field - INTERVAL '0.001' SECOND"
      )
    )
  forAll(testDataGenerateTableSchema) { (schemaStr: String, tableDesc: String) =>
    assertResult(
      tableDesc
    )(
      QuerySetup.generateTableSchema(new Schema.Parser().parse(schemaStr),
        (_, _) => ("field", new lang.StringBuilder("field type"))).toString
    )
  }

}
