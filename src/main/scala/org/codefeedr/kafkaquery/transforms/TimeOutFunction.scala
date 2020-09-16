package org.codefeedr.kafkaquery.transforms

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Function which throws an alert flag after a specified amount of time without having received new records has passed.
  * @param timeOut delay after which an alert flag is thrown (in ms)
  * @param func function to be executed on timer trigger
  */
class TimeOutFunction( // delay after which an alert flag is thrown
    val timeOut: Long,
    val func: () => Unit = () => System.exit(0)
) extends KeyedProcessFunction[java.lang.Byte, Row, Boolean] {
  // state to remember the last timer set
  private var lastTimer: ValueState[Long] = _

  override def open(conf: Configuration): Unit = { // setup timer state
    val lastTimerDesc =
      new ValueStateDescriptor[Long]("lastTimer", classOf[Long])
    lastTimer = getRuntimeContext.getState(lastTimerDesc)
  }

  @throws[Exception]
  override def processElement(
      value: Row,
      ctx: KeyedProcessFunction[java.lang.Byte, Row, Boolean]#Context,
      out: Collector[Boolean]
  ): Unit = { // get current time and compute timeout time
    val currentTime = ctx.timerService.currentProcessingTime
    val timeoutTime = currentTime + timeOut
    // register timer for timeout time
    ctx.timerService.registerProcessingTimeTimer(timeoutTime)
    // remember timeout time
    lastTimer.update(timeoutTime)
  }

  @throws[Exception]
  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[java.lang.Byte, Row, Boolean]#OnTimerContext,
      out: Collector[Boolean]
  ): Unit = { // check if this was the last timer we registered
    if (timestamp == lastTimer.value) { // it was, so no data was received afterwards.
      // fire an alert.
      out.collect(true)
      func()
    }
  }
}
