package org.constellation.storage

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.primitives.Schema.AddressCacheData
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class AddressServiceTest extends FreeSpec with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private var addressService: AddressService[IO] = _

  val src = "abc"
  val dst = "def"
  val tx =
    Fixtures.manuallyCreateTransaction(src, dst, "", "", Seq.empty, 5L, LastTransactionRef.empty, 1L.some)

  before {
    addressService = new AddressService[IO]
  }

  "clear" - {
    "should remove all addresses" in {
      addressService.putUnsafe("abcd", AddressCacheData(100, 100)).unsafeRunSync()
      addressService.putUnsafe("efgh", AddressCacheData(100, 100)).unsafeRunSync()

      addressService.clear.unsafeRunSync()

      addressService.toMap.unsafeRunSync().size shouldBe 0
    }
  }

  "transfer" - {
    "should correctly set source and destination address balances" in {
      (addressService.putUnsafe(src, AddressCacheData(100L, 100L)) >>
        addressService.putUnsafe(dst, AddressCacheData(100L, 100L)) >>
        addressService.transfer(tx)).unsafeRunSync

      val expected = Map(src -> AddressCacheData(94L, 100L), dst -> AddressCacheData(105L, 100L))
      val result = addressService.toMap.unsafeRunSync

      result shouldBe expected
    }
  }

  "transferSnapshot" - {
    "should correctly set src and dst address balances by latest snapshot" in {
      (addressService.putUnsafe(src, AddressCacheData(100L, 100L, balanceByLatestSnapshot = 100L)) >>
        addressService.putUnsafe(dst, AddressCacheData(100L, 100L, balanceByLatestSnapshot = 100L)) >>
        addressService.transferSnapshot(tx)).unsafeRunSync

      val expected = Map(
        src -> AddressCacheData(100L, 100L, balanceByLatestSnapshot = 94L),
        dst -> AddressCacheData(100L, 100L, balanceByLatestSnapshot = 105L)
      )
      val result = addressService.toMap.unsafeRunSync

      result shouldBe expected
    }
  }
}
