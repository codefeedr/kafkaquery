package org.kafkaquery.transforms

import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class SimpleSchemaGeneratorTest
    extends AnyFunSuite
    with TableDrivenPropertyChecks {

  test("All in one") {
    val schema = new Schema.Parser().parse("""{
                                |  "type" : "record",
                                |  "name" : "PyPiReleaseExt",
                                |  "namespace" : "org.codefeedr.plugins.pypi.protocol.Protocol",
                                |  "fields" : [ {
                                |    "name" : "title",
                                |    "type" : "string"
                                |  }, {
                                |    "name" : "link",
                                |    "type" : "string"
                                |  }, {
                                |    "name" : "description",
                                |    "type" : "string"
                                |  }, {
                                |    "name" : "pubDate",
                                |    "type" : "string",
                                |    "rowtime" : "true"
                                |  }, {
                                |    "name" : "project",
                                |    "type" : {
                                |      "type" : "record",
                                |      "name" : "PyPiProject",
                                |      "fields" : [ {
                                |        "name" : "info",
                                |        "type" : {
                                |          "type" : "record",
                                |          "name" : "Info",
                                |          "fields" : [ {
                                |            "name" : "author",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "author_email",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "bugtrack_url",
                                |            "type" : [ "null", "string" ]
                                |          }, {
                                |            "name" : "classifiers",
                                |            "type" : {
                                |              "type" : "array",
                                |              "items" : "string"
                                |            }
                                |          }, {
                                |            "name" : "description",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "description_content_type",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "docs_url",
                                |            "type" : [ "null", "string" ]
                                |          }, {
                                |            "name" : "download_url",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "downloads",
                                |            "type" : {
                                |              "type" : "record",
                                |              "name" : "Downloads",
                                |              "fields" : [ {
                                |                "name" : "last_day",
                                |                "type" : "int"
                                |              }, {
                                |                "name" : "last_month",
                                |                "type" : "int"
                                |              }, {
                                |                "name" : "last_week",
                                |                "type" : "int"
                                |              } ]
                                |            }
                                |          }, {
                                |            "name" : "home_page",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "keywords",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "favouriteColors",
                                |            "type" : {
                                |                "type" : "map",
                                |                "values" : "string"
                                |             }
                                |          }, {
                                |            "name" : "license",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "maintainer",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "maintainer_email",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "name",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "package_url",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "platform",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "project_url",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "project_urls",
                                |            "type" : [ "null", {
                                |              "type" : "record",
                                |              "name" : "ProjectUrl",
                                |              "fields" : [ {
                                |                "name" : "Homepage",
                                |                "type" : "string"
                                |              } ]
                                |            } ]
                                |          }, {
                                |            "name" : "release_url",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "requires_dist",
                                |            "type" : {
                                |              "type" : "array",
                                |              "items" : "string"
                                |            }
                                |          }, {
                                |            "name" : "requires_python",
                                |            "type" : [ "null", "string" ]
                                |          }, {
                                |            "name" : "summary",
                                |            "type" : "string"
                                |          }, {
                                |            "name" : "version",
                                |            "type" : "string"
                                |          } ]
                                |        }
                                |      }, {
                                |        "name" : "last_serial",
                                |        "type" : "long"
                                |      }, {
                                |        "name" : "releases",
                                |        "type" : {
                                |          "type" : "array",
                                |          "items" : {
                                |            "type" : "record",
                                |            "name" : "ReleaseVersion",
                                |            "fields" : [ {
                                |              "name" : "version",
                                |              "type" : "string"
                                |            }, {
                                |              "name" : "releases",
                                |              "type" : {
                                |                "type" : "array",
                                |                "items" : {
                                |                  "type" : "record",
                                |                  "name" : "Release",
                                |                  "fields" : [ {
                                |                    "name" : "comment_text",
                                |                    "type" : "string"
                                |                  }, {
                                |                    "name" : "digests",
                                |                    "type" : {
                                |                      "type" : "record",
                                |                      "name" : "Digest",
                                |                      "fields" : [ {
                                |                        "name" : "md5",
                                |                        "type" : "string"
                                |                      }, {
                                |                        "name" : "sha256",
                                |                        "type" : "string"
                                |                      } ]
                                |                    }
                                |                  }, {
                                |                    "name" : "downloads",
                                |                    "type" : "double"
                                |                  }, {
                                |                    "name" : "filename",
                                |                    "type" : "string"
                                |                  }, {
                                |                    "name" : "has_sig",
                                |                    "type" : "boolean"
                                |                  }, {
                                |                    "name" : "md5_digest",
                                |                    "type" : "string"
                                |                  }, {
                                |                    "name" : "packagetype",
                                |                    "type" : "string"
                                |                  }, {
                                |                    "name" : "python_version",
                                |                    "type" : "string"
                                |                  }, {
                                |                    "name" : "requires_python",
                                |                    "type" : [ "null", "string" ]
                                |                  }, {
                                |                    "name" : "size",
                                |                    "type" : "double"
                                |                  }, {
                                |                    "name" : "upload_time",
                                |                    "type" : {
                                |                      "type" : "string",
                                |                      "isRowtime" : true
                                |                    }
                                |                  }, {
                                |                    "name" : "url",
                                |                    "type" : "string"
                                |                  } ]
                                |                }
                                |              }
                                |            } ]
                                |          }
                                |        }
                                |      }, {
                                |        "name" : "urls",
                                |        "type" : {
                                |          "type" : "array",
                                |          "items" : "Release"
                                |        }
                                |      } ]
                                |    }
                                |  } ]
                                |}""".stripMargin)
    assert(
      SimpleSchemaGenerator
        .getSimpleSchema(schema)
        .equals(
          """PyPiReleaseExt : RECORD
                                                                    |     title : STRING
                                                                    |     link : STRING
                                                                    |     description : STRING
                                                                    |     pubDate : STRING
                                                                    |     project : RECORD
                                                                    |          info : RECORD
                                                                    |               author : STRING
                                                                    |               author_email : STRING
                                                                    |               bugtrack_url : STRING (NULLABLE)
                                                                    |               classifiers : ARRAY<STRING>
                                                                    |               description : STRING
                                                                    |               description_content_type : STRING
                                                                    |               docs_url : STRING (NULLABLE)
                                                                    |               download_url : STRING
                                                                    |               downloads : RECORD
                                                                    |                    last_day : INT
                                                                    |                    last_month : INT
                                                                    |                    last_week : INT
                                                                    |               home_page : STRING
                                                                    |               keywords : STRING
                                                                    |               favouriteColors : MAP<STRING>
                                                                    |               license : STRING
                                                                    |               maintainer : STRING
                                                                    |               maintainer_email : STRING
                                                                    |               name : STRING
                                                                    |               package_url : STRING
                                                                    |               platform : STRING
                                                                    |               project_url : STRING
                                                                    |               project_urls : RECORD (NULLABLE)
                                                                    |                    Homepage : STRING
                                                                    |               release_url : STRING
                                                                    |               requires_dist : ARRAY<STRING>
                                                                    |               requires_python : STRING (NULLABLE)
                                                                    |               summary : STRING
                                                                    |               version : STRING
                                                                    |          last_serial : LONG
                                                                    |          releases : ARRAY<RECORD>
                                                                    |          urls : ARRAY<RECORD>
                                                                    |""".stripMargin
            .replace("\r\n", "\n")
        )
    )
  }
}
