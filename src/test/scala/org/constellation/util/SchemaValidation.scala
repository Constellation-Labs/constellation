package org.constellation.util

import org.constellation.primitives.SensorData
import org.scalatest.FlatSpec
import org.json4s._
import org.json4s.jackson.JsonMethods._
import constellation._

class SchemaValidation extends FlatSpec {

  private val schema = SensorData.schema
  private val validator = SensorData.validator


  "Sample schema" should "validate" in {

    val validExample = SensorData(5, "ASDFG")
    assert(validator.validate(schema, asJsonNode(decompose(validExample))).isSuccess)

  }

  "Sample schema" should "not validate" in {

    val invalidExample = SensorData(500, "asdfg")
    assert(!validator.validate(schema, asJsonNode(decompose(invalidExample))).isSuccess)



  }
}
