package org.constellation.primitives

import cats.syntax.all._
import org.constellation.Fixtures
import org.constellation.domain.transaction.LastTransactionRef
import org.scalatest.{FreeSpec, Matchers}

class TransactionTest extends FreeSpec with Matchers {
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
