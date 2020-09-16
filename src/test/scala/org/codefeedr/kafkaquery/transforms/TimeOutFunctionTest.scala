package org.codefeedr.kafkaquery.transforms

import com.google.common.collect.Lists
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class TimeOutFunctionTest extends AnyFunSuite with BeforeAndAfter {

  private var testHarness: OneInputStreamOperatorTestHarness[Row, Boolean] = _
  private var timeOutFunction: TimeOutFunction = _

  before {
    timeOutFunction = new TimeOutFunction(1 * 1000, () => Unit) // 1 s timeout

    testHarness = new KeyedOneInputStreamOperatorTestHarness[java.lang.Byte, Row, Boolean](
      new KeyedProcessOperator(timeOutFunction),
      new NullByteKeySelector[Row],
      Types.BYTE)

    testHarness.open()
  }

  test("processElement") {
    testHarness.processElement(new Row(0), 5)

    assert(testHarness.extractOutputStreamRecords().isEmpty)
  }

  test("onTimerTriggered") {
    testHarness.processElement(new Row(0), 1)
    testHarness.setProcessingTime(1050)
    testHarness.processElement(new Row(0), 2)

    assert(testHarness.extractOutputStreamRecords() == Lists.newArrayList(new StreamRecord(true)))
  }

  test("onTimerNotTriggered") {
    testHarness.processElement(new Row(0), 1)
    testHarness.setProcessingTime(500)
    testHarness.processElement(new Row(0), 2)

    assert(testHarness.extractOutputStreamRecords().isEmpty)
  }
}
