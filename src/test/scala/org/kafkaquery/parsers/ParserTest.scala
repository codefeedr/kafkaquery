package org.kafkaquery.parsers

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.kafkaquery.parsers.Configurations.Mode
import org.kafkaquery.util.ZookeeperSchemaExposer
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ParserTest extends AnyFunSuite with EmbeddedKafka with BeforeAndAfter {

  private val subjectName = "testSubject"
  private val subjectSchema = new Schema.Parser()
    .parse("""{"type":"record","name":"testCC","fields":[{"name":"s","type":"string"}]}""")
  private var parser: Parser = _
  private var outStream: ByteArrayOutputStream = _

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 0,
    zooKeeperPort = 0
  )

  val format: String =
    """
      |{
      |  "type" : "record",
      |  "name" : "NpmRelease",
      |  "namespace" : "org.codefeedr.plugins.npm.protocol.Protocol",
      |  "fields" : [ {
      |    "name" : "name",
      |    "type" : "string"
      |  }, {
      |    "name" : "retrieveDate",
      |    "type" : {
      |      "type" : "string",
      |      "isRowtime" : true
      |    }
      |  } ]
      |}
      |""".stripMargin

  val npmTableSchema: Schema = new Schema.Parser().parse(
    format
  )

  before {
    parser = new Parser()
    outStream = new ByteArrayOutputStream()
  }

  test("parseNothing") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      assertThrows[RuntimeException] {
        parser.parse(null)
      }
    }
  }

  test("parseDefinedPlusParseEmpty") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(subjectSchema, subjectName)

      parser.parse(Array("-t", subjectName))
      assert(parser.getSchemaExposer.get(subjectName).isDefined)
    }
  }

  test("printAllTopics") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(subjectSchema, subjectName)

      //check whether the TopicParser prints the same output after more than 1 call.
      Console.withOut(outStream)(parser.printTopics())
      val res = new String(outStream.toByteArray)
      val otherOutStream = new java.io.ByteArrayOutputStream
      Console.withOut(otherOutStream)(parser.printTopics())
      val res2 = new String(outStream.toByteArray)
      assert(res.equals(res2))
    }
  }

  test("setKafkaAddress") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      val kafkaAddress = "someAddress"
        val parserConfig = parser.parseConfig(("--kafka " + kafkaAddress + " --zookeeper \"notworkingAddress\"").split(" ")).get
      assert(parserConfig.kafkaAddress == kafkaAddress)
    }
  }

  test("setZooKeeperAddress") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      val ZKAddress = "someOTherAddress"
      val parserConfig = parser.parseConfig(("--zookeeper "+ ZKAddress).split(" ")).get
      assert(parserConfig.zookeeperAddress == ZKAddress)
    }
  }


  test("updateSchemaFromFile") {
    withRunningKafkaOnFoundPort(config) { implicit config =>

      val fileName = "schema"
      val zkAddress = s"localhost:${config.zooKeeperPort}"
      val avroSchema = """{"type":"record","name":"Person","namespace":"org.codefeedr.plugins.repl.org.kafkaquery.parsers.Parser.updateSchema","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"},{"name":"city","type":"string"}]}"""
      new PrintWriter(fileName) {write(avroSchema); close()}

      parser.parse(("--update-schema "+ subjectName +"=" + fileName+ " --zookeeper "+zkAddress).split(" "))

      assert(parser.getSchemaExposer.get(subjectName).get.toString.equals(avroSchema))

      FileUtils.deleteQuietly(new File(fileName))
    }
  }

  test("updateSchemaParserFailure") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      val avroSchema = """incorrect Avro Format"""
      Console.withErr(outStream) {
        parser.updateSchema(subjectName, avroSchema)
        assertResult("Error while parsing the given schema.") {
          outStream.toString().trim
        }
      }
    }
  }

  test("checkConfigQuery") {
    val args: Seq[String] = Seq(
      "-q", "select * from topic"
    )

    val parsed = parser.parseConfig(args)

    assertResult("select * from topic") {
      parsed.get.queryConfig.query
    }
    assertResult(Mode.Query) {
      parsed.get.mode
    }
  }

  test("testPrintSchema") {
    val topic = "World"

    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))
      parser.getSchemaExposer.put(npmTableSchema, topic)

      Console.withOut(outStream) {

        parser.printAvroSchema(topic)

        assertResult(format.replaceAll("[\r\n]", "")) {
          outStream.toString().replaceAll("[\r\n]", "")
        }
      }
    }
  }

  test("testFailToPrintEmptyTopicSchema") {
    withRunningKafkaOnFoundPort(config) { implicit config =>
      parser.setSchemaExposer(new ZookeeperSchemaExposer(s"localhost:${config.zooKeeperPort}"))

      Console.withErr(outStream) {

        parser.printAvroSchema("Hello")

        assertResult("Schema of topic Hello is not defined.") {
          outStream.toString().trim
        }
      }
    }
  }
}
