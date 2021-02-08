package org.kafkaquery.sinks

import java.io.PrintStream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}

class SimplePrintSinkFunction[T](stdErr: Boolean = false)
    extends RichSinkFunction[T] {

  private val writer = new SimplePrintSinkOutputWriter[T](stdErr)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    writer.open()
  }

  override def invoke(value: T, context: SinkFunction.Context): Unit =
    writer.write(value)

}

class SimplePrintSinkOutputWriter[T](stdErr: Boolean) extends Serializable {
  @transient private var stream: PrintStream = _

  def open(): Unit = { // get the target stream
    stream =
      if (stdErr) System.err
      else System.out
  }

  def write(record: T): Unit = {
    stream.println(record.toString)
  }
}
