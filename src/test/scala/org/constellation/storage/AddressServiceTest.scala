package org.constellation.storage

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.transaction.LastTransactionRef
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class AddressServiceTest
    extends AnyFreeSpec
    with BeforeAndAfter
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private var addressService: AddressService[IO] = _

  val src = "abc"
  val dst = "def"

  val tx =
    Fixtures.manuallyCreateTransaction(src, dst, "", "", Seq.empty, 5L, LastTransactionRef.empty, 1L.some)

  before {
    addressService = new AddressService[IO]
  }

  "setAll" - {
    "should clear old data" in {
      val oldKey = "foo"
      addressService.set(oldKey, AddressCacheData(100, 100)).unsafeRunSync()

      val balances = Map(
        src -> AddressCacheData(200, 200),
        dst -> AddressCacheData(100, 100)
      )
      addressService.setAll(balances).unsafeRunSync()

      addressService.getAll.unsafeRunSync() shouldEqual balances
      addressService.lookup(oldKey).unsafeRunSync() shouldBe None
    }
  }

  "clear" - {
    "should remove all addresses" in {
      addressService.set("abcd", AddressCacheData(100, 100)).unsafeRunSync()
      addressService.set("efgh", AddressCacheData(100, 100)).unsafeRunSync()

      addressService.clear.unsafeRunSync()

      addressService.getAll.unsafeRunSync().size shouldBe 0
    }
  }

  "transferTransaction" - {
    "should correctly set source and destination address balances" in {
      (addressService.set(src, AddressCacheData(100L, 100L)) >>
        addressService.set(dst, AddressCacheData(100L, 100L)) >>
        addressService.transferTransaction(tx)).unsafeRunSync

      val expected = Map(src -> AddressCacheData(94L, 100L), dst -> AddressCacheData(105L, 100L))
      val result = addressService.getAll.unsafeRunSync

      result shouldBe expected
    }
  }

  "transferSnapshotTransaction" - {
    "should correctly set src and dst address balances by latest snapshot" in {
      (addressService.set(src, AddressCacheData(100L, 100L, balanceByLatestSnapshot = 100L)) >>
        addressService.set(dst, AddressCacheData(100L, 100L, balanceByLatestSnapshot = 100L)) >>
        addressService.transferSnapshotTransaction(tx)).unsafeRunSync

      val expected = Map(
        src -> AddressCacheData(100L, 100L, balanceByLatestSnapshot = 94L),
        dst -> AddressCacheData(100L, 100L, balanceByLatestSnapshot = 105L)
      )
      val result = addressService.getAll.unsafeRunSync

      result shouldBe expected
    }
  }
}
