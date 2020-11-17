package org.constellation.storage

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.Fixtures
import org.constellation.checkpoint.CheckpointService
import org.constellation.schema.address.Address
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.serialization.KryoSerializer
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class RateLimitingTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with BeforeAndAfter {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val logger = Slf4jLogger.getLogger[IO]

  KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()

  "update" - {
    "it should increment counter for all (not dummy) source addresses in provided txs" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 2, Address("b") -> 1)
    }

    "it should blacklist addresses which has no available transactions left" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      rl.update(List(tx1, tx2)).unsafeRunSync

      rl.blacklisted.lookup("a").unsafeRunSync shouldBe -1.some
    }

    "it should ignore dummy transactions" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeDummyTransaction("a", "z", Fixtures.tempKey)
      val tx3 = Fixtures.makeDummyTransaction("b", "z", Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 1)
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

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2)).unsafeRunSync

      val cb1 = mock[CheckpointCache]
      cb1.checkpointBlock shouldReturn mock[CheckpointBlock]
      cb1.checkpointBlock.baseHash shouldReturn "cb1"
      cb1.checkpointBlock.transactions shouldReturn List(tx3)

      cs.fullData("cb1") shouldReturnF cb1.some

      rl.reset(List("cb1"))(cs).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map(Address("a") -> 1)
      rl.blacklisted.lookup("a").unsafeRunSync shouldBe -1.some
    }

    "it should ignore dummy transactions" in {
      val rl = RateLimiting[IO]()
      val cs = mock[CheckpointService[IO]]

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeDummyTransaction("a", "z", Fixtures.tempKey)
      val tx3 = Fixtures.makeDummyTransaction("b", "z", Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      val tx4 = Fixtures.makeDummyTransaction("a", "z", Fixtures.tempKey)
      val tx5 = Fixtures.makeDummyTransaction("b", "z", Fixtures.tempKey)
      val tx6 = Fixtures.makeDummyTransaction("c", "z", Fixtures.tempKey)

      val cb1 = mock[CheckpointCache]
      cb1.checkpointBlock shouldReturn mock[CheckpointBlock]
      cb1.checkpointBlock.baseHash shouldReturn "cb1"
      cb1.checkpointBlock.transactions shouldReturn List(tx4, tx5, tx6)

      cs.fullData("cb1") shouldReturnF cb1.some

      rl.reset(List("cb1"))(cs).unsafeRunSync

      rl.counter.get.unsafeRunSync shouldBe Map()
    }
  }

  "available" - {
    "it should return how many available transactions left for account" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2)).unsafeRunSync

      val limit = rl.limits.txsPerAddressPerSnapshot

      rl.available(Address("a")).unsafeRunSync shouldBe (limit - 1)
      rl.available(Address("b")).unsafeRunSync shouldBe (limit - 1)
    }

    "it should return full available limit if there are no counters" in {
      val rl = RateLimiting[IO]()

      val limit = rl.limits.txsPerAddressPerSnapshot

      rl.available(Address("unknown")).unsafeRunSync shouldBe limit
    }

    "it should return full available limit if there are some counters but address is unknown" in {
      val rl = RateLimiting[IO]()

      val tx1 = Fixtures.makeTransaction("a", "z", 5L, Fixtures.tempKey)
      val tx2 = Fixtures.makeTransaction("b", "z", 5L, Fixtures.tempKey)
      val tx3 = Fixtures.makeTransaction("c", "z", 5L, Fixtures.tempKey)

      rl.update(List(tx1, tx2, tx3)).unsafeRunSync

      val limit = rl.limits.txsPerAddressPerSnapshot

      rl.available(Address("unknown")).unsafeRunSync shouldBe limit
    }
  }
}
