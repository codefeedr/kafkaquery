package org.kafkaquery.util

import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class UserInputRetrieverTest extends AnyFunSuite with MockitoSugar {


  test("Simple character retrieval") {
    val inputReaderMock = mock[UserInputRetriever.InputReadWrapper]
    when(inputReaderMock.readChar())
      .thenReturn('k').andThen('o')

    UserInputRetriever.reader = inputReaderMock

    assert('o' == UserInputRetriever.readAllowedChar(List('o', 'm')))
  }
}
