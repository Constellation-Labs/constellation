package org.constellation.storage

import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.{ConstellationContextShift, ConstellationExecutionContext, DAO, Fixtures}
import org.constellation.primitives.{Transaction, TransactionCacheData}
import org.constellation.storage.ConsensusStatus.ConsensusStatus
import org.constellation.storage.transactions.TransactionStatus
import org.constellation.storage.transactions.TransactionStatus.TransactionStatus
import org.constellation.util.Metrics
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class TransactionServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {
  implicit val contextShift: ContextShift[IO] = ConstellationContextShift.global
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  var dao: DAO = _
  var txService: TransactionService[IO] = _

  val hash = "ipsum"
  val tx = mock[TransactionCacheData]
  tx.hash shouldReturn hash

  before {
    dao = mockDAO
    txService = new TransactionService[IO](dao)
  }

  "put" - {
    "should add a transaction as pending by default" in {
      txService.put(tx).unsafeRunSync

      txService.pending.lookup(tx.hash).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to add a pending transaction" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.pending.lookup(tx.hash).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to add an arbitrary transaction" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.arbitrary.lookup(tx.hash).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to add an accepted transaction" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.accepted.lookup(tx.hash).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to add an unknown transaction" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync

      txService.unknown.lookup(tx.hash).unsafeRunSync shouldBe Some(tx)
    }

    "should not allow to add an inConsensus transaction" in {
      an[Exception] should be thrownBy txService.put(tx, ConsensusStatus.InConsensus).unsafeRunSync
    }

    "should raise an exception if transaction status is unknown" in {
      an[Exception] should be thrownBy txService.put(tx, "foo".asInstanceOf[ConsensusStatus]).unsafeRunSync
    }
  }

  "update" - {
    val tx2 = mock[TransactionCacheData]
    tx2.transaction shouldReturn mock[Transaction]
    tx2.hash shouldReturn hash

    "should update a pending transaction" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.update(hash, _ => tx2).unsafeRunSync

      txService.pending.lookup(hash).unsafeRunSync shouldBe Some(tx2)
    }

    "should update an arbitrary transaction" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.update(hash, _ => tx2).unsafeRunSync

      txService.arbitrary.lookup(hash).unsafeRunSync shouldBe Some(tx2)
    }

    "should update an inConsensus transaction" in {
      txService.inConsensus.put(hash, tx).unsafeRunSync

      txService.update(hash, _ => tx2).unsafeRunSync

      txService.inConsensus.lookup(hash).unsafeRunSync shouldBe Some(tx2)
    }

    "should update an accepted transaction" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.update(hash, _ => tx2).unsafeRunSync

      txService.accepted.lookup(hash).unsafeRunSync shouldBe Some(tx2)
    }

    "should update an unknown transaction" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync

      txService.update(hash, _ => tx2).unsafeRunSync

      txService.unknown.lookup(hash).unsafeRunSync shouldBe Some(tx2)
    }
  }

  "lookup" - {
    "should lookup for a pending transaction" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.lookup(hash).unsafeRunSync shouldBe Some(tx)
    }

    "should lookup for an arbitrary transaction" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.lookup(hash).unsafeRunSync shouldBe Some(tx)
    }

    "should lookup for an inConsensus transaction" in {
      txService.inConsensus.put(hash, tx).unsafeRunSync

      txService.lookup(hash).unsafeRunSync shouldBe Some(tx)
    }

    "should lookup for an accepted transaction" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.lookup(hash).unsafeRunSync shouldBe Some(tx)
    }

    "should lookup for an unknown transaction" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync

      txService.lookup(hash).unsafeRunSync shouldBe Some(tx)
    }

    "should return None if transaction does not exist" in {
      txService.lookup("123123").unsafeRunSync shouldBe None
    }

    "should allow to lookup for pending transaction only" in {
      txService.lookup(hash, ConsensusStatus.Pending).unsafeRunSync shouldBe None
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync
      txService.lookup(hash, ConsensusStatus.Pending).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to lookup for arbitrary transaction only" in {
      txService.lookup(hash, ConsensusStatus.Arbitrary).unsafeRunSync shouldBe None
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync
      txService.lookup(hash, ConsensusStatus.Arbitrary).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to lookup for inConsensus transaction only" in {
      txService.lookup(hash, ConsensusStatus.InConsensus).unsafeRunSync shouldBe None
      txService.inConsensus.put(hash, tx).unsafeRunSync
      txService.lookup(hash, ConsensusStatus.InConsensus).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to lookup for accepted transaction only" in {
      txService.lookup(hash, ConsensusStatus.Accepted).unsafeRunSync shouldBe None
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync
      txService.lookup(hash, ConsensusStatus.Accepted).unsafeRunSync shouldBe Some(tx)
    }

    "should allow to lookup for unknown transaction only" in {
      txService.lookup(hash, ConsensusStatus.Unknown).unsafeRunSync shouldBe None
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync
      txService.lookup(hash, ConsensusStatus.Unknown).unsafeRunSync shouldBe Some(tx)
    }

    "should raise an exception if transaction status is unknown" in {
      an[Exception] should be thrownBy txService.lookup(hash, "foo".asInstanceOf[ConsensusStatus]).unsafeRunSync
    }

  }

  "getArbitrary" - {
    "should return all arbitrary transactions" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.getArbitrary.unsafeRunSync shouldBe Map(hash -> tx)
    }
  }

  "contains" - {
    "should return true if transaction exists as pending transaction" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.contains(hash).unsafeRunSync shouldBe true
    }

    "should return true if transaction exists as arbitrary transaction" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.contains(hash).unsafeRunSync shouldBe true
    }

    "should return true if transaction exists as inConsensus transaction" in {
      txService.inConsensus.put(hash, tx).unsafeRunSync

      txService.contains(hash).unsafeRunSync shouldBe true
    }

    "should return true if transaction exists as accepted transaction" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.contains(hash).unsafeRunSync shouldBe true
    }

    "should return true if transaction exists as unknown transaction" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync

      txService.contains(hash).unsafeRunSync shouldBe true
    }

    "should return false if transaction does not exist" in {
      txService.contains(hash).unsafeRunSync shouldBe false
    }
  }

  "isAccepted" - {
    "should return true if transaction is accepted" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.isAccepted(hash).unsafeRunSync shouldBe true
    }

    "should return false if transaction is not accepted" in {
      txService.isAccepted(hash).unsafeRunSync shouldBe false
    }
  }

  "accept" - {
    "should put transaction to accepted storage" in {
      txService.accept(tx).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Accepted).unsafeRunSync shouldBe Some(tx)
    }

    "should remove transaction from inConsensus storage" in {
      txService.inConsensus.put(hash, tx).unsafeRunSync

      txService.accept(tx).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.InConsensus).unsafeRunSync shouldBe None
    }

    "should remove transaction from unknown storage" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync

      txService.accept(tx).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Unknown).unsafeRunSync shouldBe None
    }

    "should remove transaction from arbitrary storage" in {
      txService.put(tx, ConsensusStatus.Arbitrary).unsafeRunSync

      txService.accept(tx).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Arbitrary).unsafeRunSync shouldBe None
    }
  }

  "applySnapshot" - {
    "should remove merkleRoot hash from merklePool" in {
      val merkleRoot = "merkleRootHash"
      txService.merklePool.put(merkleRoot, Seq(hash)).unsafeRunSync

      txService.applySnapshot(List(tx), merkleRoot).unsafeRunSync

      txService.merklePool.lookup(merkleRoot).unsafeRunSync shouldBe None
    }

    "should remove accepted transactions" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      txService.applySnapshot(List(tx), "aaa").unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Accepted).unsafeRunSync shouldBe None
    }
  }

  "returnTransactionsToPending" - {
    "should move transactions from inConsensus to pending storage" in {
      txService.inConsensus.put(hash, tx).unsafeRunSync
      txService.returnToPending(List(hash)).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.InConsensus).unsafeRunSync shouldBe None
      txService.lookup(hash, ConsensusStatus.Pending).unsafeRunSync shouldBe Some(tx)
    }

    "should not move transactions from other than inConsensus storage" in {
      txService.put(tx, ConsensusStatus.Unknown).unsafeRunSync
      txService.returnToPending(List(hash)).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Unknown).unsafeRunSync shouldBe Some(tx)
      txService.lookup(hash, ConsensusStatus.Pending).unsafeRunSync shouldBe None
    }
  }

  "pullForConsensusSafe" - {
    "should be safe to use concurrently" in {
      val pullsIteration = 100
      val pullsMinCount = 50

      val totalExpected = pullsIteration * pullsMinCount

      val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
      val cs = IO.contextShift(ec)

      val puts = (1 to totalExpected).toList
        .map(_ => constellation.createTransaction(Fixtures.id1.address, Fixtures.id2.address, 1L, Fixtures.tempKey))
        .map(TransactionCacheData(_))
        .traverse(tx => cs.shift *> txService.put(tx))

      val pulls = (1 to pullsIteration).toList
        .map(_ => cs.shift *> txService.pullForConsensus(pullsMinCount))

      // Fill minimum txs required
      puts.unsafeRunSync()
      txService.pending.size().unsafeRunSync() shouldBe totalExpected

      // Run puts in background
      puts.unsafeRunAsyncAndForget()

      val results = {
        implicit val shadedEc: ExecutionContext = ec
        Await.result(Future.sequence(pulls.map(_.unsafeToFuture())), 5 seconds).map(_.map(_.transaction.hash))
      }

      // Should always pull txs
      results.count(_.isEmpty) shouldBe 0
      results.flatten.size shouldBe totalExpected
      results.flatten.distinct.size shouldBe totalExpected
    }
  }

  "pullForConsensus" - {
    "should remove a transaction from pending storage" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.pullForConsensus(1).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.Pending).unsafeRunSync shouldBe None
    }

    "should add a transaction to inConsensus storage" in {
      txService.put(tx, ConsensusStatus.Pending).unsafeRunSync

      txService.pullForConsensus(1).unsafeRunSync

      txService.lookup(hash, ConsensusStatus.InConsensus).unsafeRunSync shouldBe Some(tx)
    }
  }

  "getLast20Accepted" - {
    "should return last 20 accepted transactions" in {
      txService.put(tx, ConsensusStatus.Accepted).unsafeRunSync

      def mockTx(n: Int): TransactionCacheData = {
        val tcd = mock[TransactionCacheData]

        tcd.transaction shouldReturn mock[Transaction]
        tcd.transaction.hash shouldReturn n.toString

        tcd
      }

      val txs = (1 to 30).toList.map(mockTx)

      txs.traverse(tx => txService.put(tx, ConsensusStatus.Accepted)).unsafeRunSync

      txService.getLast20Accepted.unsafeRunSync shouldBe txs.reverse.take(20).toList
    }
  }

  "findHashesByMerkleRoot" - {
    "should lookup for merkleRoot hash in merklePool" in {
      val merkleRoot = "merkleRootHash"

      txService.merklePool.put(merkleRoot, List("a", "b")).unsafeRunSync

      txService.findHashesByMerkleRoot(merkleRoot).unsafeRunSync shouldBe Some(List("a", "b"))
    }
  }

  private def mockDAO: DAO = {
    val dao = mock[DAO]

    dao.metrics shouldReturn mock[Metrics]
    dao.metrics.incrementMetricAsync[IO](*) shouldReturnF Unit

    dao
  }
}
