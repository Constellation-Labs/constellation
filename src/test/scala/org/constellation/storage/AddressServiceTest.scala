package org.constellation.storage

import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.constellation.primitives.Schema.AddressCacheData
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class AddressServiceTest extends FreeSpec with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private var addressService: AddressService[IO] = _

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
}
