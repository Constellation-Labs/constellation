package org.constellation

import java.security.KeyPair

import org.constellation.crypto.KeyUtils._

import org.scalatest.FlatSpec

// case class Test(a: EdgeHashType, b: EdgeHashType) // tmp comment

/** Various utility tests.
  *
  * @todo Test CB serializations.
  * @todo Rewrite/Reenable tests.
  */
class UtilityTest extends FlatSpec {

  "Bundles" should "serialize and deserialize properly with json" in {
    implicit val kp: KeyPair = makeKeyPair()

    /* // tmp comment
    assert(b3.json.x[Bundle] == b3)
    */
  }

  "BigInt hash" should "XOR properly as a distance metric" in {
    /* // tmp comment
    // Use BigInt hex for dumping key hashes later.
    val hash = Fixtures.transaction3.hash
    val hash2 = Fixtures.transaction4.hash
    println(hash)
    println(hash2)
    val bi = BigInt(hash, 16)
    val bi2 = BigInt(hash2, 16)
    val xor = bi ^ bi2
    println(bi)
    println(bi2)
    println(xor)
    val xorHash = xor.toString(16)
    println(xorHash)

    val xor2 = bi ^ BigInt(Fixtures.transaction2.hash, 16)
    println(xor > xor2)
    */
  }

  "Case object serialization" should "work" in {
    /* // tmp comment
    val t = Test(TXHash, AsdfHash)
    println(t.j)
    println(t.j.x[Test])
    assert(t.j.x[Test] == t)
    */
  }

} // end UtilityTest
