package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect.{Concurrent, IO}
import cats.implicits._
import constellation.signedObservationEdge
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.transaction.TransactionService.createTransactionEdge
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, TransactionEdgeData, TypedEdgeHash}
import org.constellation.primitives.{Edge, Schema, Transaction}
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

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

  "getLastTransactionRef" - {
    "should return empty string and 0 ordinal if address is unknown" in {
      val last = service.getLastTransactionRef("unknown")

      last.unsafeRunSync shouldBe LastTransactionRef.empty
    }

    "should return last transaction ref if address is known" in {
      val address = "DAG123"
      val lastTxRef = LastTransactionRef("tx123", 1)
      val init = service.lastTransactionRef.set(Map(address -> lastTxRef))
      val getLastTxRef = service.getLastTransactionRef(address)
      val last = (init >> getLastTxRef).unsafeRunSync

      last shouldBe lastTxRef
    }
  }

  "getLastAcceptedTransactionRef" - {
    "should return empty string and 0 ordinal if address is unknown" in {
      val lastAccepted = service.getLastAcceptedTransactionRef("unknown").unsafeRunSync

      lastAccepted shouldBe LastTransactionRef.empty
    }

    "should return last accepted transaction ref if address is known" in {
      val address = "DAG123"
      val lastTxRef = LastTransactionRef("tx123", 1)
      val init = service.lastAcceptedTransactionRef.set(Map(address -> lastTxRef))
      val getLastAccepted = service.getLastAcceptedTransactionRef(address)
      val lastAccepted = (init >> getLastAccepted).unsafeRunSync

      lastAccepted shouldBe lastTxRef
    }
  }

  "getLastAcceptedTransactionMap" - {
    "should return current map of accepted transaction refs" in {
      val lastAcceptedTxRef = Map("DAG123" -> LastTransactionRef("abc", 1))
      val init = service.lastAcceptedTransactionRef.set(lastAcceptedTxRef)
      val getAcceptedTransactions = service.getLastAcceptedTransactionMap()
      val result = (init >> getAcceptedTransactions).unsafeRunSync

      result shouldBe lastAcceptedTxRef
    }
  }

  "acceptTransaction" - {
    "for dummy transaction" - {
      "should not persist transaction ref as last accepted" in {
        val dummyTxEdge = mock[Edge[TransactionEdgeData]]
        val dummyTx = Transaction(dummyTxEdge, LastTransactionRef.empty, isDummy = true)
        val acceptTx = service.acceptTransaction(dummyTx)
        val getLastAccepted = service.lastAcceptedTransactionRef.get
        val result = (acceptTx >> getLastAccepted).unsafeRunSync

        result shouldBe Map.empty
      }
    }
    "for normal transaction" - {
      "should persist transaction ref as last accepted" in {
        val src = "DAG123"
        val dst = "DAG456"
        val txEdge = createTransactionEdge(src, dst, LastTransactionRef.empty, 1L, Fixtures.tempKey)
        val tx = Transaction(txEdge, LastTransactionRef.empty)
        val acceptTx = service.acceptTransaction(tx)
        val getLastAccepted = service.lastAcceptedTransactionRef.get
        val result = (acceptTx >> getLastAccepted).unsafeRunSync

        result shouldBe Map(src -> LastTransactionRef(tx.hash, 1L))
      }
    }
  }

  "createAndSetLastTransaction" - {
    "should return new transaction" in {
      val src = "unknown"
      val dst = "bb"
      val createdTx = service
        .createAndSetLastTransaction(src, dst, 1L, Fixtures.tempKey, false)
        .unsafeRunSync

      createdTx.src.address shouldBe src
      createdTx.dst.address shouldBe dst
      createdTx.lastTxRef shouldBe LastTransactionRef.empty
      createdTx.ordinal shouldBe 1L
      createdTx.isDummy shouldBe false
      createdTx.amount shouldBe 100000000L
    }

    "should assign last transaction ref correctly when creating new transaction" in {
      val src = "unknown"
      val dst = "bb"
      val createdTx = service
        .createAndSetLastTransaction(src, dst, 1L, Fixtures.tempKey, false)
        .unsafeRunSync

      val newTx = service
        .createAndSetLastTransaction(src, dst, 1L, Fixtures.tempKey, false)
        .unsafeRunSync

      newTx.lastTxRef shouldBe LastTransactionRef(createdTx.hash, createdTx.ordinal)
      newTx.ordinal shouldBe 2L
    }

    "should set the last transaction ref properly" in {
      val src = "unknown"
      val createdTx = service
        .createAndSetLastTransaction(src, "bb", 1L, Fixtures.tempKey, false)
        .unsafeRunSync

      val lastTx = service.lastTransactionRef.get.unsafeRunSync.getOrElse(src, Map.empty)

      lastTx shouldBe LastTransactionRef(createdTx.hash, createdTx.ordinal)
    }
  }

  "applySnapshotInfo" - {
    "should override map of last accepted transactions" in {
      val lastAcceptedTransactionRef = Map("DAG123" -> LastTransactionRef("abc", 1), "DAG456"-> LastTransactionRef("def", 1))
      val snapshotInfo = mock[SnapshotInfo]
      snapshotInfo.lastAcceptedTransactionRef shouldReturn lastAcceptedTransactionRef
      val applySnapshotInfo = service.applySnapshotInfo(snapshotInfo)
      val getLastAcceptedTx = service.lastAcceptedTransactionRef.get
      val result = (applySnapshotInfo >> getLastAcceptedTx).unsafeRunSync

      result shouldBe lastAcceptedTransactionRef
    }
  }

  "createDummyTransaction" - {
    val src = "DAG123"
    val dst = "DAG456"
    "should create a dummy transaction with an empty transaction ref even if isDummy is set to false" in {
      val tx = service
        .createDummyTransaction(src, dst, 1L, Fixtures.tempKey, false)
        .unsafeRunSync

      tx.isDummy shouldBe true
      tx.lastTxRef shouldBe LastTransactionRef.empty
      tx.src.address shouldBe src
      tx.dst.address shouldBe dst
      tx.amount shouldBe 100000000L
    }
  }

  "clear" - {
    "should clear persisted data of last transactions and last accepted transactions refs" in {
      val lastTxRef = Map("DAG123" -> LastTransactionRef("abc", 1))
      val lastAcceptedTxRef = Map("DAG456" -> LastTransactionRef("def", 1))
      val init = service.lastTransactionRef.set(lastTxRef) >>
        service.lastAcceptedTransactionRef.set(lastAcceptedTxRef) >>
        service.clear
      init.unsafeRunSync

      service.lastTransactionRef.get.unsafeRunSync shouldBe Map.empty
      service.lastAcceptedTransactionRef.get.unsafeRunSync shouldBe Map.empty
    }
  }

  def createTransaction(
    src: String,
    dst: String
  ): IO[Transaction] = {
    val txData = TransactionEdgeData(1L, LastTransactionRef("", 0L))

    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash(dst, EdgeHashType.AddressHash)),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(Fixtures.tempKey)

    for {
      last <- service.getLastTransactionRef(src)
      tx = Transaction(Edge(oe, soe, txData), LastTransactionRef(last.prevHash, last.ordinal))
    } yield tx
  }

}
