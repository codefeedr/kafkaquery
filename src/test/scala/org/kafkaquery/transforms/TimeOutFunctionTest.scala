package org.kafkaquery.transforms

import org.apache.flink.streaming.api.operators.ProcessOperator
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class TimeOutFunctionTest extends AnyFunSuite with BeforeAndAfter {

  private var testHarness: OneInputStreamOperatorTestHarness[Row, Unit] = _
  private var timeOutFunction: TimeOutFunction = _

  private var funcExecuted: Boolean = _
  private val timeoutValMs = 1 * 1000

  before {
    funcExecuted = false
    timeOutFunction = new TimeOutFunction(timeoutValMs, () => {
      funcExecuted = true
      Unit
    }) // 1 s timeout

    testHarness = new OneInputStreamOperatorTestHarness[Row, Unit](
      new ProcessOperator(timeOutFunction))

    testHarness.open()
  }

  test("processElement") {
    testHarness.processElement(new Row(0), 5)

    assert(!funcExecuted)
  }

  test("onTimerTriggered") {
    testHarness.processElement(new Row(0), 1)
    Thread.sleep(timeoutValMs + 50)
    testHarness.processElement(new Row(0), 2)

    assert(funcExecuted)
  }

  test("onTimerNotTriggered") {
    testHarness.processElement(new Row(0), 1)
    Thread.sleep(timeoutValMs / 2)
    testHarness.processElement(new Row(0), 2)

    assert(!funcExecuted)
  }
}
