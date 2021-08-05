package org.constellation.keytool

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeterministicKeyGenTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  test("Two invocations of deterministic key gen should generate identical keys") {
    val seed = "reallysecretbrainwalletpassword"
    val keyPairsOne = DeterministicKeyGen.makeKeyPairsLetterMap(10, seed)
    val keyPairsTwo = DeterministicKeyGen.makeKeyPairsLetterMap(10, seed)

    keyPairsOne.map(KeyUtils.keyPairToAddress) shouldBe keyPairsTwo.map(KeyUtils.keyPairToAddress)
  }
}
