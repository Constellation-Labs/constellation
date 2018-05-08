package org.constellation.tx

import org.constellation.Fixtures
import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Schema.{SignedTX, TX}

class TXTest extends FlatSpec {

  "TX" should "sign" in {


    import Fixtures._

    val address1 = pubKeysToAddress(Seq(tempKey.getPublic, tempKey1.getPublic))
    val address2 = pubKeyToAddress(tempKey2.getPublic)

    val tx = SignedTX(
      TX(
        Seq(address1, address2),
        tempKey3.getPublic,
        1L,
        keyMap = Seq(0, 0, 1)
      ).multiSigned()(Seq(tempKey, tempKey1, tempKey2))
    )

    assert(tx.valid)

    val tx2 = SignedTX(
      TX(
        Seq(address1, address2),
        tempKey3.getPublic,
        1L,
        keyMap = Seq(0, 0, 1)
      ).multiSigned()(Seq(tempKey, tempKey1, tempKey5))
    )

    assert(!tx2.valid)

  }

}
