package org.codefeedr.kafkaquery.transforms

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.util.{Timer, TimerTask}

/** Function which throws an alert flag after a specified amount of time without having received new records has passed.
  * @param timeOut delay after which an alert flag is thrown (in ms)
  * @param func function to be executed on timer trigger
  */
class TimeOutFunction( // delay after which an alert flag is thrown
    val timeOut: Int,
    val func: () => Unit
) extends ProcessFunction[Row, Unit] {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    TimerController.refresh(timeOut, func)
  }

  override def processElement(
      value: Row,
      ctx: ProcessFunction[Row, Unit]#Context,
      out: Collector[Unit]
  ): Unit = {
    TimerController.refresh(timeOut, func)
  }
}

private object TimerController {
  private var timer: Timer = new Timer()

  def refresh(timeOut: Int, func: () => Unit): Unit = this.synchronized {
    timer.cancel()
    timer = new Timer()
    timer.schedule(
      new TimerTask {
        override def run(): Unit = func()
      },
      timeOut
    )
  }
}
