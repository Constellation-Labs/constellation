package org.constellation.primitives

import cats.syntax.all._
import org.constellation.Fixtures
import org.constellation.schema.v2.transaction.LastTransactionRef
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TransactionTest extends AnyFreeSpec with Matchers {
  "feeValue" - {
    "should return 0 for negative fee" in {
      val negativeFee = (-10L).some
      val tx = Fixtures.manuallyCreateTransaction("", "", "", "", Seq.empty, 0L, LastTransactionRef.empty, negativeFee)
      tx.feeValue shouldBe 0L
    }

    "should return 0 if fee is not set" in {
      val tx = Fixtures.manuallyCreateTransaction()
      tx.feeValue shouldBe 0L
    }

    "should return fee's value for positive fee" in {
      val positiveFee = 10L.some
      val tx = Fixtures.manuallyCreateTransaction("", "", "", "", Seq.empty, 0L, LastTransactionRef.empty, positiveFee)
      tx.feeValue shouldBe 10L
    }
  }
}
