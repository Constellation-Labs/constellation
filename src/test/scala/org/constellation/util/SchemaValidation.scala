package org.constellation.util

import constellation._
import org.constellation.primitives.SensorData
import org.json4s.jackson.JsonMethods._
import org.scalatest.FlatSpec

class SchemaValidation extends FlatSpec {

  private val schema = SensorData.schema
  private val validator = SensorData.validator

  "Sample schema" should "validate" in {

    val validExample = SensorData(5, "ASDFG", "channel_id")
    assert(validator.validate(schema, asJsonNode(decompose(validExample))).isSuccess)

  }

  "Sample schema" should "not validate" in {

    val invalidExample = SensorData(500, "asdfg", "channel_id")
    assert(!validator.validate(schema, asJsonNode(decompose(invalidExample))).isSuccess)

  }
}
