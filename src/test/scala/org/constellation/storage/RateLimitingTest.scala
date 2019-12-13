package org.constellation.storage

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{FreeSpec, Matchers}
import org.constellation.Fixtures
import org.constellation.checkpoint.CheckpointService
import org.constellation.primitives.CheckpointBlock
import org.constellation.primitives.Schema.{Address, CheckpointCache}

import scala.concurrent.ExecutionContext

class RateLimitingTest extends FreeSpec with IdiomaticMockito with IdiomaticMockitoCats with Matchers {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val logger = Slf4jLogger.getLogger[IO]

  // TODO: Uncomment with re-enabling RateLimiting
  /*

  "update" - {
    "it should increment counter for all source addresses in provided txs" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 2, Address("b") -> 1)
    }

    "it should blacklist addresses which has no available transactions left" in {
      val rl = RateLimiting[IO]()

      val txs = (1 to 50).toList.map(_ => Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey))
      rl.update(txs).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 50)
      rl.blacklisted.lookup("a").unsafeRunSync shouldBe -1.some
    }
  }

  "reset" - {
    "it should recalculate counters using all cbHashes left after snapshot" in {
      val rl = RateLimiting[IO]()
      val cs = mock[CheckpointService[IO]]

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      val tx4 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx5 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)
      val tx6 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)
      val tx7 = Fixtures.makeTransaction("c", "z", 5L, Fixtures.tempKey)

      val cb1 = mock[CheckpointCache]
      cb1.checkpointBlock shouldReturn mock[CheckpointBlock]
      cb1.checkpointBlock.baseHash shouldReturn "cb1"
      cb1.checkpointBlock.transactions shouldReturn List(tx4, tx5, tx6, tx7)

      cs.fullData("cb1") shouldReturnF cb1.some

      rl.reset(List("cb1"))(cs).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 1, Address("b") -> 2, Address("c") -> 1)
    }

    "it should blacklist addresses which has no available transactions left" in {
      val rl = RateLimiting[IO]()
      val cs = mock[CheckpointService[IO]]

      val txs = (1 to 60).toList.map(_ => Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey))
      rl.update(txs).unsafeRunSync

      val cb1 = mock[CheckpointCache]
      cb1.checkpointBlock shouldReturn mock[CheckpointBlock]
      cb1.checkpointBlock.baseHash shouldReturn "cb1"
      cb1.checkpointBlock.transactions shouldReturn txs.take(55)

      cs.fullData("cb1") shouldReturnF cb1.some

      rl.reset(List("cb1"))(cs).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 55)
      rl.blacklisted.lookup("a").unsafeRunSync shouldBe -6.some
    }
  }

  "available" - {
    "it should return how many available transactions left for account" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      val left = rl.limits.available / 2

      rl.available(Address("a")).unsafeRunSync shouldBe (left - 2)
    }

    "it should return full available limit if there are no counters" in {
      val rl = RateLimiting[IO]()

      val left = rl.limits.available

      rl.available(Address("unknown")).unsafeRunSync shouldBe left
    }

    "it should return full available limit if there are some counters but address is unknown" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      val left = rl.limits.available / 2

      rl.available(Address("unknown")).unsafeRunSync shouldBe left
    }
  }

 */
}
