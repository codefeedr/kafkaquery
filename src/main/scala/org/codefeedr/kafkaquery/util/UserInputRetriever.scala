package org.codefeedr.kafkaquery.util

import scala.annotation.tailrec
import scala.io.StdIn

object UserInputRetriever {

  var reader: InputReadWrapper = new InputReadWrapper

  /** Retrieves a character from the reader.
    * @param allowedChars characters that are accepted answers
    * @return the retrieved character
    */
  @tailrec
  def readAllowedChar(allowedChars: List[Char]): Char = {
    println(
      "Please insert one of the following characters: " + allowedChars.toSet
    )
    val input = reader.readChar()
    if (allowedChars.contains(input))
      return input
    readAllowedChar(allowedChars)
  }

  /** Wrapping Standard input for testing, with Mockito 2 it will be possible to mock final classes.
    */
  class InputReadWrapper {
    var inputStream: StdIn.type = StdIn

    def readChar(): Char = {
      inputStream.readChar()
    }
  }
}
