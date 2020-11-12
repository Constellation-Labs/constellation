package org.constellation.regression

import org.constellation.Fixtures
import org.constellation.schema.v2.signature.{SignHelp, SingleHashSignature}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

// TODO: Consider moving to hash signature test suite
class SingleHashSignatureTest extends AnyFreeSpec with Matchers {
  "#995" - {
    "single hash signature validation" - {
      "should check the hash correctness" in {
        val keyPair = Fixtures.tempKey
        val hash = "loremipsum"

        val signature = SignHelp.hashSign(hash, keyPair)
        val shs = SingleHashSignature("differenthash", signature)

        shs.valid(hash) shouldBe false
      }
    }
  }
}
