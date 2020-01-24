package org.constellation.util

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class AccountBalanceCSVReaderTest extends FunSuite {

  val src = getClass.getResource("/test.csv")

  test("Load CSV file with account balances and normalize") {
    val values = new AccountBalanceCSVReader(src.getPath).read()

    values.size shouldBe 2

    values.head.accountHash shouldBe "abcd"
    values.head.balance shouldBe 12345678L

    values.last.accountHash shouldBe "efgh"
    values.last.balance shouldBe 187654321L
  }
}
