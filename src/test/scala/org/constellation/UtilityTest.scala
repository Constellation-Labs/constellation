package org.constellation

import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Block

class UtilityTest extends FlatSpec {

  "Blocks" should "serialize and deserialize properly with json" in {

    import Fixtures2._
    val ser = latestBlock4B.json
    val deser = ser.x[Block]
    assert(latestBlock4B == deser)

  }

}
