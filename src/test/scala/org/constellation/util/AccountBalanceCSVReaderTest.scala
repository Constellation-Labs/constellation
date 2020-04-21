package org.constellation.util

import org.scalatest.{FreeSpec, Matchers}

class AccountBalanceCSVReaderTest extends FreeSpec with Matchers {

  private val nonNormalizedCSV = getClass.getResource("/test.csv")
  private val normalizedCSV = getClass.getResource("/test_normalized.csv")

  "Loads normalized file" in {
    val values = new AccountBalanceCSVReader(nonNormalizedCSV.getPath, false).read()

    values.size shouldBe 2

    values.head.accountHash shouldBe "abcd"
    values.head.balance shouldBe 12345678L

    values.last.accountHash shouldBe "efgh"
    values.last.balance shouldBe 187654321L
  }

  "Loads non-normalized file and normalizes" in {
    val values = new AccountBalanceCSVReader(normalizedCSV.getPath, true).read()

    values.size shouldBe 2

    values.head.accountHash shouldBe "abcd"
    values.head.balance shouldBe 12345678L

    values.last.accountHash shouldBe "efgh"
    values.last.balance shouldBe 187654321L
  }
}
