package org.constellation

import org.constellation.util.{SignHelp, SingleHashSignature}
import org.scalatest.{FreeSpec, Matchers}

class SingleHashSignatureTest extends FreeSpec with Matchers {
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
