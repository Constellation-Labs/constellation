package org.constellation.tx

import org.constellation.primitives.Schema.SendToAddress

import org.scalatest.FlatSpec

/** Documentation. */
class TXTests extends FlatSpec {

  "TX hashes" should "split evenly" in {

  }

  "Send json request" should "parse correctly" in {

    import constellation._

    assert("""{"dst": "asdf", "amount": 1}""".x[SendToAddress] == SendToAddress("asdf", 1))

    assert(
      """{"dst": "asdf", "amount": 1, "normalized": false}""".x[SendToAddress] ==
        SendToAddress("asdf", 1, normalized = false)
    )

  }

}

