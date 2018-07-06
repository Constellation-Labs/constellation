package org.constellation

import java.security.KeyPair

import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Schema.{Bundle, BundleData, ParentBundleHash, TransactionHash}

class UtilityTest extends FlatSpec {

  "Bundles" should "serialize and deserialize properly with json" in {

    implicit val kp: KeyPair = makeKeyPair()
    val b = Bundle(BundleData(Seq(TransactionHash("a"), ParentBundleHash("b"))).signed())
    val b2 = Bundle(BundleData(Seq(TransactionHash("c"), ParentBundleHash("b"))).signed())
    val b3 = Bundle(BundleData(Seq(b, b2)).signed())
    println(b3.json)
    assert(b3.json.x[Bundle] == b3)
  }

  "BigInt hash" should "XOR properly as a distance metric" in {
/*

    // Use bigint hex for dumping key hashes later.
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

}
