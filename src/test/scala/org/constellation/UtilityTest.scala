package org.constellation

import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Block

class UtilityTest extends FlatSpec {

  "Blocks" should "serialize and deserialize properly with json" in {

    import Fixtures._
    val ser = latestBlock4B.json
    val deser = ser.x[Block]
    assert(latestBlock4B == deser)

  }

  "BigInt hash" should "XOR properly as a distance metric" in {

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

  }

}
