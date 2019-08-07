package org.constellation.util

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class AccountBalanceCSVReaderTest extends FunSuite {

  test("Load CSV file with account balances") {
    val values = new AccountBalanceCSVReader("src/test/resources/test.csv").read()

    values.size shouldBe 2

    values.head.accountHash shouldBe "abcd"
    values.head.balance shouldBe 1000L

    values.last.accountHash shouldBe "efgh"
    values.last.balance shouldBe 2000L
  }
}
