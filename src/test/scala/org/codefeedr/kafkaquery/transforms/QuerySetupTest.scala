package org.codefeedr.kafkaquery.transforms

import java.lang
import org.apache.avro.Schema
import org.codefeedr.kafkaquery.parsers.Configurations.{EarliestQueryStart, LatestQueryStart, QueryStart}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}

class QuerySetupTest extends AnyFunSuite with BeforeAndAfter with TableDrivenPropertyChecks {

  test("Topics should correctly be extracted from query") {
    val res = QuerySetup.extractTopics("SELECT * from t1, t2, t3", List("t2", "t3", "t4"))
    assert(res == List("t2", "t3"))
  }

  val testDataGetTableCreationCommand: TableFor1[QueryStart] =
    Table(
      "startStrategy",

      EarliestQueryStart(),
      LatestQueryStart()
    )
  forAll(testDataGetTableCreationCommand) { (startStrategy: QueryStart) =>
    val tableName = "t1"
    val tableFields = "(f1 INT, f2 STRING)"
    val kafkaAddr = "localhost:9092"

    assertResult(
      s"CREATE TEMPORARY TABLE `$tableName` ($tableFields) WITH ('connector' = 'kafka', " +
        s"'topic' = '$tableName', 'properties.bootstrap.servers' = '$kafkaAddr', " +
        s"'properties.group.id' = 'kq', 'scan.startup.mode' = '${startStrategy.getProperty}', " +
        "'properties.default.api.timeout.ms' = '5000', 'format' = 'json', " +
        "'json.timestamp-format.standard' = 'ISO-8601', 'json.ignore-parse-errors' = 'true', " +
        "'json.fail-on-missing-field' = 'false')"
    )(
      QuerySetup.getTableCreationCommand(tableName, new java.lang.StringBuilder(tableFields), kafkaAddr,
        startStrategy = startStrategy, ignoreParseErr = true)
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
        "field field type, field field type, `kafka_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
          "WATERMARK FOR `kafka_time` AS `kafka_time` - INTERVAL '0.001' SECOND"
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
