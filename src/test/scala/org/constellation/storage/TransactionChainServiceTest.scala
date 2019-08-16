package org.constellation.storage

import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, FunSuite, Matchers}

class TransactionChainServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val cs = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  var service: TransactionChainService[IO] = _

  before {
    service = TransactionChainService[IO]
  }

  "getNext" - {
    "should return (\"\", 1) if the address is unknown" in {
      val next = service.getNext("unknown")

      next.unsafeRunSync shouldBe ("", 1)
    }

    "should return stored count and previous hash if the address is known" in {
      service.lastTransactionCount.set(4).unsafeRunSync
      service.lastTxHash.set(Map("known" -> "previous")).unsafeRunSync
      val next = service.getNext("known")

      next.unsafeRunSync shouldBe ("previous", 5)
    }

    "should increment generated count" in {
      val next1 = service.getNext("unknown")
      val next2 = service.getNext("unknown")

      next1.unsafeRunSync shouldBe ("", 1)
      next2.unsafeRunSync shouldBe ("", 2)
    }
  }

  "getLatest" - {
    "should return 0 if the address is unknown" in {
      val count = service.getLatest("unknown")

      count.unsafeRunSync shouldBe 0
    }

    "should return stored count if the address is known" in {
      service.addressCount.set(Map("known" -> 10)).unsafeRunSync

      val count = service.getLatest("known")

      count.unsafeRunSync shouldBe 10
    }

    "lastTransactionHash" - {
      "should return None if the address is unknown" in {
        val hash = service.lastTransactionHash("unknown")

        hash.unsafeRunSync shouldBe none
      }

      "should return the last transaction hash if the address is known" in {
        service.lastTxHash.set(Map("known" -> "last")).unsafeRunSync

        val hash = service.lastTransactionHash("known")

        hash.unsafeRunSync shouldBe "last".some
      }
    }

    "observeTransaction" - {
      "should increment the address by 1 if the address is unknown" in {
        val observe = service.observeTransaction("unknown", "foo")
        val count = service.getLatest("unknown")

        (observe *> count).unsafeRunSync shouldBe 1
      }

      "should increment the address by 1 if the address is known" in {
        service.addressCount.set(Map("known" -> 15)).unsafeRunSync

        val observe = service.observeTransaction("known", "foo")
        val count = service.getLatest("known")

        (observe *> count).unsafeRunSync shouldBe 16
      }

      "should return 1 if the address is unknown" in {
        val observe = service.observeTransaction("unknown", "foo")

        observe.unsafeRunSync shouldBe (1, "foo")
      }

      "should return incremented value if the address is known" in {
        service.addressCount.set(Map("known" -> 15)).unsafeRunSync

        val observe = service.observeTransaction("known", "foo")

        observe.unsafeRunSync shouldBe (16, "foo")
      }

      "should set the last transaction hash if the address is unknown" in {
        val observe = service.observeTransaction("unknown", "bar")
        val hash = service.lastTransactionHash("unknown")

        (observe *> hash).unsafeRunSync shouldBe "bar".some
      }

      "should update the last transaction hash if the address is known" in {
        service.addressCount.set(Map("known" -> 15)).unsafeRunSync
        service.lastTxHash.set(Map("known" -> "foo")).unsafeRunSync

        val observe = service.observeTransaction("known", "bar")
        val hash = service.lastTransactionHash("known")

        (observe *> hash).unsafeRunSync shouldBe "bar".some
      }
    }
  }

}
