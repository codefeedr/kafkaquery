package org.codefeedr.kafkaquery.transforms

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/** Function which throws an alert flag after a specified amount of time without having received new records has passed.
  * @param timeOut delay after which an alert flag is thrown (in ms)
  * @param func function to be executed on timer trigger
  */
class TimeOutFunction( // delay after which an alert flag is thrown
    val timeOut: Long,
    val func: () => Unit
) extends KeyedProcessFunction[java.lang.Byte, Row, Boolean] {
  // state to remember the last timer set
  private var lastTimer: ValueState[Long] = _
  private var timerOffset: ValueState[Long] = _
  private var isTimerRunning: ValueState[Boolean] = _

  override def open(conf: Configuration): Unit = { // setup timer state

    val lastTimerDesc =
      new ValueStateDescriptor[Long]("lastTimer", classOf[Long])
    lastTimer = getRuntimeContext.getState(lastTimerDesc)

    val timerOffsetDesc =
      new ValueStateDescriptor[Long]("timerOffset", classOf[Long])
    timerOffset = getRuntimeContext.getState(timerOffsetDesc)

    val isTimerRunningDesc =
      new ValueStateDescriptor[Boolean]("isTimerRunning", classOf[Boolean])
    isTimerRunning = getRuntimeContext.getState(isTimerRunningDesc)
  }

  @throws[Exception]
  override def processElement(
      value: Row,
      ctx: KeyedProcessFunction[java.lang.Byte, Row, Boolean]#Context,
      out: Collector[Boolean]
  ): Unit = {
    // get current time and compute timeout time
    val currentTime = ctx.timerService.currentProcessingTime
    val timeoutTime = currentTime + timeOut

    if (isTimerRunning.value()) { //if a timer is running already, update the offset
      timerOffset.update(lastTimer.value() - currentTime)
    } else { //if no timer is running create new Timer
      ctx.timerService.registerProcessingTimeTimer(timeoutTime)

      //set last timer to our new one
      lastTimer.update(timeoutTime)
      //reset offset
      timerOffset.update(0)

      isTimerRunning.update(true)
    }
  }

  @throws[Exception]
  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[java.lang.Byte, Row, Boolean]#OnTimerContext,
      out: Collector[Boolean]
  ): Unit = {
    if (timerOffset.value() == 0) { // Offset is 0 if no element was processed while this timer was running -> Timeout reached!
      // fire an alert.
      out.collect(true)
      func()
    } else { //Timer expired but elements were processed while timer was running -> create new timer with corresponding offset
      val currentTime = ctx.timerService.currentProcessingTime
      val timeoutTime =
        currentTime + timeOut - timerOffset
          .value() //A fresh timer that ends when: latest element time + timeout is reached
      ctx.timerService.registerProcessingTimeTimer(timeoutTime)

      //set last timer to our new one
      lastTimer.update(timeoutTime)
      //reset offset
      timerOffset.update(0)
    }
  }
}
