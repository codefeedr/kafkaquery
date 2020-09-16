/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.codefeedr.kafkaquery.util

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class ZookeeperSchemaExposerTest extends AnyFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with EmbeddedKafka {

  val schema = """{
                   "type": "record",
                   "name": "User",
                   "fields": [
                     {
                         "name": "name",
                         "type": "string"
                     },
                     {
                         "name": "age",
                         "type": "int"
                     }
                   ]
                 }"""

  val differentSchema = """{
                   "type": "record",
                   "name": "Different_User",
                   "fields": [
                     {
                         "name": "full_name",
                         "type": "string"
                     },
                     {
                         "name": "age",
                         "type": "int"
                     }
                   ]
                 }"""

  var exposer: ZookeeperSchemaExposer = _
  var parsedSchema: Schema = _
  var differentParsedSchema: Schema = _

  val subject = "testSubject"

  override def beforeAll(): Unit = {
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(zooKeeperPort = 2181)
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  before {
    exposer = new ZookeeperSchemaExposer("localhost:2181")

    parsedSchema = exposer
      .parse(schema)
      .get

    differentParsedSchema = exposer
      .parse(differentSchema)
      .get
  }

  after {
    exposer.deleteAll()
  }

  test("A simple schema should be correctly saved") {
    //put the schema
    exposer.put(parsedSchema, subject)

    //if I get the schema, it should be the same
    assert(exposer.get(subject).get == parsedSchema)
    assert(exposer.get(subject).get != differentParsedSchema)
  }

  test("A simple schema should be correctly overwritten") {
    //ensure schema's are not the same
    assert(differentParsedSchema != parsedSchema)

    //put the schema
    exposer.put(parsedSchema, subject)

    //if I get the schema, it should be the same
    assert(exposer.get(subject).get == parsedSchema)

    //put the different schema
    exposer.put(differentParsedSchema, subject)

    //if I get the schema, it should not be the same as the original
    assert(exposer.get(subject).get != parsedSchema)
    assert(exposer.get(subject).get == differentParsedSchema)
  }

  test("A simple schema should be correctly deleted") {
    //put the schema
    assert(exposer.put(parsedSchema, subject))

    //it should be properly deleted
    assert(exposer.delete(subject))
  }

  test("A simple schema cannot be deleted if it is not there") {
    //it should be properly deleted
    assert(!exposer.delete(subject))
  }

  test("Get a schema on a non existent subject should return None") {
    assert(exposer.get("IDoNoTeXiSt").isEmpty)
  }

  test("An invalid schema should return None") {
    assert(exposer.parse("iNVaLIdScHemA{}$%:)").isEmpty)
  }

  test("All schema's should be properly deleted") {
    //put the schema
    assert(exposer.put(parsedSchema, subject))
    exposer.deleteAll()
    assert(exposer.get(subject).isEmpty)
  }

  test("All schema's should be properly deleted even if called twice") {
    //put the schema
    assert(exposer.put(parsedSchema, subject))
    exposer.deleteAll()
    exposer.deleteAll()
    assert(exposer.get(subject).isEmpty)
  }

}
