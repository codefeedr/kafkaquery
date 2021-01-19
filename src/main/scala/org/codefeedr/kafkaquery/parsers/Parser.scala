package org.codefeedr.kafkaquery.parsers

import java.io.File
import java.nio.charset.Charset

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.KeeperException
import org.codefeedr.kafkaquery.commands.QueryCommand
import org.codefeedr.kafkaquery.parsers.Configurations.{Config, Mode}
import org.codefeedr.kafkaquery.transforms.JsonToAvroSchema
import org.codefeedr.kafkaquery.util.{
  KafkaRecordRetriever,
  ZookeeperSchemaExposer
}
import scopt.OptionParser

import scala.io.Source

class Parser extends OptionParser[Config]("codefeedr") {

  head("Codefeedr CLI", "1.0.0")

  opt[String]('q', "query")
    .valueName("<query>")
    .action((x, c) => {
      c.copy(mode = Mode.Query, queryConfig = c.queryConfig.copy(query = x))
    })
    .text(
      s"Allows querying available data sources through Flink SQL. " +
        s"query - valid Flink SQL query. More information about Flink SQL can be found at: https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html. " +
        s""
    )
    .children(
      opt[Int]('p', "port")
        .valueName("<port>")
        .action((x, c) => c.copy(queryConfig = c.queryConfig.copy(port = x)))
        .text(
          s"Writes the output data of the given query to a socket which gets created with the specified port. " +
            s"Local connection with the host can be done by e.g. netcat."
        ),
      opt[String]('k', "kafka_topic")
        .valueName("<kafka-topic>")
        .action((x, c) =>
          c.copy(queryConfig = c.queryConfig.copy(outTopic = x))
        )
        .text(
          s"Writes the output data of the given query to the specified Kafka topic. " +
            s"If the Kafka topic does not exist, it will be created."
        ),
      opt[Int]('t', "timeout")
        .valueName("<seconds>")
        .action((x, c) => c.copy(queryConfig = c.queryConfig.copy(timeout = x)))
        .text(
          s"Specifies a timeout in seconds. If no message is received for the duration of the timeout the program terminates."
        ),
      opt[Unit]("from-earliest")
        .action((_, c) =>
          c.copy(queryConfig = c.queryConfig.copy(checkEarliest = true))
        )
        .text(
          s"Specifies that the data is consumed from the earliest offset." +
            s"If no state is specified the query results will be printed from EARLIEST."
        ),
      opt[Unit]("from-latest")
        .action((_, c) =>
          c.copy(queryConfig = c.queryConfig.copy(checkLatest = true))
        )
        .text(
          s"Specifies that the data is consumed from the latest offset."
        ),
      checkConfig(c =>
        if (c.queryConfig.checkEarliest && c.queryConfig.checkLatest)
          failure("Cannot start from earliest and latest.")
        else success
      ),
      checkConfig(c =>
        if (c.queryConfig.outTopic.nonEmpty && c.queryConfig.port != -1)
          failure("Cannot write query output to both kafka-topic and port.")
        else success
      )
    )
  opt[String]("topic")
    .valueName("<topic_name>")
    .action((x, c) => c.copy(mode = Mode.Topic, topicName = x))
    .text(
      "Output the specified topic's schema which entails the field names and types."
    )
  opt[Unit]("topics")
    .action((_, c) => c.copy(mode = Mode.Topics))
    .text("List all topic names for which a schema is available.")
  opt[(String, File)]("schema")
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
          case Mode.Topic  => printSchema(config.topicName)
          case Mode.Topics => printTopics()
          case Mode.Schema =>
            updateSchema(config.topicName, config.avroSchema)
          case Mode.Infer => inferSchema(config.topicName, config.kafkaAddress)
          case _ =>
            Console.err.println("Command not recognized.")
        }
      case _ => Console.err.println("Unknown configuration.")
    }
  }

  /** Updates the AvroSchema in Zookeeper for the specified topic
    *
    * @param topicName    topic Name
    * @param schemaString an Avro Schema in String format.
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

  /** List all topic names stored in Zookeeper.
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

  /** Prints the schema associated with the topic
    *
    * @param topicName name of the topic in zookeeper
    */
  def printSchema(topicName: String): Unit = {
    val schema = zookeeperExposer.get(topicName)
    if (schema.isDefined) {
      println(schema.get.toString(true))
    } else {
      Console.err.println(s"Schema of topic $topicName is not defined.")
    }
  }

  /** Initialises the ZookeeperSchemaExposer.
    *
    * @return success of the initialisation
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
    * @param topicName    name of the topic to be inferred
    * @param kafkaAddress address of the kafka instance where the topic is present
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
