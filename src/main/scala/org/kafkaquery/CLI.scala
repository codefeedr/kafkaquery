package org.kafkaquery

import org.kafkaquery.parsers.Parser

object CLI {

  /** Main method which looks up which arguments to parse.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {

    new Parser().parse(args)
  }
}
