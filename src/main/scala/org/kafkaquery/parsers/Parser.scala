package org.kafkaquery.parsers

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.KeeperException
import org.kafkaquery.commands.QueryCommand
import org.kafkaquery.parsers.Configurations._
import org.kafkaquery.transforms.{JsonToAvroSchema, SimpleSchemaGenerator}
import org.kafkaquery.util.{KafkaRecordRetriever, ZookeeperSchemaExposer}
import scopt.OptionParser

import java.io.File
import java.nio.charset.Charset
import scala.io.Source

class Parser extends OptionParser[Config]("kafkaquery") {

  head("KafkaQuery CLI")

  opt[String]('q', "query")
    .valueName("<query>")
    .action((x, c) => {
      c.copy(mode = Mode.Query, queryConfig = c.queryConfig.copy(query = x))
    })
    .text(
      s"Allows querying available data sources through Flink SQL. " +
        s"query - valid Flink SQL query. More information about Flink SQL can be found at: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#operations. " +
        s""
    )
    .children(
      opt[(String, String)]('o', "output")
        .keyValueName("<sink>", "<param>")
        .text("Writes the output data of the query to the given sink.")
        .action({ case ((k, v), c) =>
          c.copy(queryConfig =
            c.queryConfig.copy(output = QueryOut.initFromString(k, v))
          )
        }),
      opt[Int]('t', "timeout")
        .valueName("<seconds>")
        .action((x, c) => c.copy(queryConfig = c.queryConfig.copy(timeout = x)))
        .text(
          s"Specifies a timeout in seconds. If no message is received for the duration of the timeout the program terminates."
        ),
      opt[String]('s', "start")
        .action((x, c) =>
          c.copy(queryConfig =
            c.queryConfig.copy(startStrategy = QueryStart.initFromString(x))
          )
        )
        .text(
          "Specifies the start strategy for retrieving records from Kafka.p"
        )
    )
  opt[String]("schema")
    .valueName("<topic_name>")
    .action((x, c) => c.copy(mode = Mode.Topic, topicName = x))
    .text(
      "Outputs the specified topic's schema which entails the field names and types."
    )
  opt[String]("simple-schema")
    .valueName("<topic_name>")
    .action((x, c) => c.copy(mode = Mode.SimpleTopic, topicName = x))
    .text(
      "Outputs the specified topic's schema in a simplified form."
    )
  opt[Unit]("topics")
    .action((_, c) => c.copy(mode = Mode.Topics))
    .text("List all topic names for which a schema is available.")
  opt[(String, File)]("update-schema")
    .keyName("<topic_name>")
    .valueName("<avro_Schema_file>")
    .action({ case ((topicName, schema), c) =>
      c.copy(
        mode = Mode.Schema,
        avroSchema =
          FileUtils.readFileToString(schema, Charset.defaultCharset()),
        topicName = topicName
      )
    })
    .text(
      "Updates the schema for the specified topic with the given Avro schema (as a file)."
    )
  opt[String]("infer-schema")
    .valueName("<topic_name>")
    .action((x, c) => c.copy(mode = Mode.Infer, topicName = x))
    .text(
      "Infers and registers an Avro schema for the specified topic."
    )
  opt[String]("kafka")
    .valueName("<Kafka_address>")
    .action((address, config) => config.copy(kafkaAddress = address))
    .text("Sets the Kafka address for the execution.")

  opt[String]("zookeeper")
    .valueName("<ZK_address>")
    .action((address, config) => config.copy(zookeeperAddress = address))
    .text("Sets the ZooKeeper address for the execution.")
  opt[Seq[File]]("udf")
    .valueName("<function_file1,function_file2...>")
    .action({ case (sequence, c) =>
      c.copy(
        queryConfig =
          c.queryConfig.copy(userFunctions = getClassNameList(sequence.toList))
      )
    })
    .text(
      "Registers the specified User defined functions for usage in queries."
    )
  help('h', "help")

  private var zookeeperExposer: ZookeeperSchemaExposer = _

  def parseConfig(args: Seq[String]): Option[Config] =
    super.parse(args, Config())

  def parse(args: Seq[String]): Unit = {
    parseConfig(args) match {
      case Some(config) =>
        initZookeeperExposer(config.zookeeperAddress)
        config.mode match {
          case Mode.Query =>
            new QueryCommand(
              config.queryConfig,
              zookeeperExposer,
              config.kafkaAddress
            ).execute()
          case Mode.Topic       => printAvroSchema(config.topicName)
          case Mode.SimpleTopic => printSimpleAvroSchema(config.topicName)
          case Mode.Topics      => printTopics()
          case Mode.Schema => updateSchema(config.topicName, config.avroSchema)
          case Mode.Infer  => inferSchema(config.topicName, config.kafkaAddress)
          case _ =>
            Console.err.println("Command not recognized.")
        }
      case _ => Console.err.println("Unknown configuration.")
    }
  }

  /** Updates the AvroSchema in ZooKeeper for the specified topic.
    *
    * @param topicName
    *   topic Name
    * @param schemaString
    *   an Avro Schema in String format
    */
  def updateSchema(topicName: String, schemaString: String): Unit = {
    var schema: Schema = null

    try {
      schema = new Schema.Parser().parse(schemaString)
    } catch {
      case _: Throwable =>
        Console.err.println("Error while parsing the given schema.")
    }
    if (schema != null)
      zookeeperExposer.put(schema, topicName)
  }

  /** List all topic names stored in ZooKeeper.
    */
  def printTopics(): Unit = {
    val children = zookeeperExposer.getAllChildren

    if (children.nonEmpty) {
      val sb = new StringBuilder()
      sb.append("Available topics:\n")

      children
        .slice(0, children.size - 1)
        .foreach(x => sb.append(x + ", "))
      sb.append(children.last)
      println(sb.toString())
    } else {
      Console.err.println("There are currently no topics available")
    }
  }

  /** Prints the Avro schema associated with the topic
    *
    * @param topicName
    *   name of the topic in ZooKeeper
    */
  def printAvroSchema(topicName: String): Unit = {
    val schema = zookeeperExposer.get(topicName)
    if (schema.isDefined) {
      println(schema.get.toString(true))
    } else {
      Console.err.println(s"Schema of topic $topicName is not defined.")
    }
  }

  /** Prints the simple Avro schema associated with the topic
    *
    * @param topicName
    *   name of the topic in ZooKeeper
    */
  def printSimpleAvroSchema(topicName: String): Unit = {
    val schema = zookeeperExposer.get(topicName)
    if (schema.isDefined) {
      println(SimpleSchemaGenerator.getSimpleSchema(schema.get))
    } else {
      Console.err.println(s"Schema of topic $topicName is not defined.")
    }
  }

  /** Initialises the ZookeeperSchemaExposer.
    *
    * @return
    *   success of the initialisation
    */
  private def initZookeeperExposer(zookeeperAddress: String): Unit = {
    try {
      zookeeperExposer = new ZookeeperSchemaExposer(zookeeperAddress)
    } catch {
      case _: KeeperException.ConnectionLossException =>
        Console.err.println("Connection to ZooKeeper lost.")
        System.exit(0)
    }
  }

  def getSchemaExposer: ZookeeperSchemaExposer = zookeeperExposer

  def setSchemaExposer(zk: ZookeeperSchemaExposer): Unit = {
    zookeeperExposer = zk
  }

  /** Executes the steps required for infering a schema.
    *
    * @param topicName
    *   name of the topic to be inferred
    * @param kafkaAddress
    *   address of the kafka instance where the topic is present
    */
  def inferSchema(topicName: String, kafkaAddress: String): Unit = {
    val schema = JsonToAvroSchema.inferSchema(
      topicName,
      new KafkaRecordRetriever(topicName, kafkaAddress)
    )

    updateSchema(topicName, schema.toString)
    println("Successfully generated schema for topic " + topicName)
  }

  def getClassNameList(classes: List[File]): List[(String, File)] = {
    classes.map(file => {
      (extractClassNameFromFile(file), file)
    })
  }

  def extractClassNameFromFile(file: File): String = {
    var last = ""
    val fileContents = Source.fromFile(file.getAbsoluteFile)
    fileContents.mkString
      .split(" ")
      .foreach(x => {
        if (last.equalsIgnoreCase("class")) {
          return x
        }
        last = x
      })
    fileContents.close()
    throw new RuntimeException("Not a valid class" + file.getName)
  }

}
