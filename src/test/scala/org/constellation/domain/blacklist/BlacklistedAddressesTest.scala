package org.constellation.domain.blacklist

import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class BlacklistedAddressesTest extends FreeSpec with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private var blacklistAddresses: BlacklistedAddresses[IO] = _

  before {
    blacklistAddresses = BlacklistedAddresses[IO]
  }

  "contains" - {
    "returns true if address exists" in {
      val address: String = "abcd1234"

      blacklistAddresses.add(address).unsafeRunSync()

      blacklistAddresses.contains(address).unsafeRunSync() shouldBe true
    }

    "return false if address doesn't exist" in {
      val address: String = "abcd1234"

      blacklistAddresses.add(address).unsafeRunSync()

      blacklistAddresses.contains("abcd").unsafeRunSync() shouldBe false
    }
  }

  "add" - {
    "should add address to blacklist" in {
      val address: String = "abcd1234"

      blacklistAddresses.add(address).unsafeRunSync()

      blacklistAddresses.get.unsafeRunSync().head shouldBe address
    }
  }

  "addAll" - {
    "should add all addresses to blacklist" in {
      val addresses = List("abcd1234", "efgh5678")

      blacklistAddresses.addAll(addresses).unsafeRunSync()

      blacklistAddresses.get.unsafeRunSync().size shouldBe 2
    }
  }
}
