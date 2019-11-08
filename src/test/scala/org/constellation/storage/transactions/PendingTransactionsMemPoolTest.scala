package org.constellation.storage.transactions

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.domain.transaction.{PendingTransactionsMemPool, TransactionChainService}
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, TransactionEdgeData, TypedEdgeHash}
import org.constellation.primitives.{Edge, Transaction, TransactionCacheData}
import org.constellation.schema.HashGenerator
import org.constellation.serializer.KryoHashGenerator
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class PendingTransactionsMemPoolTest extends FreeSpec with IdiomaticMockito with Matchers with BeforeAndAfter {
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val hashGenerator: HashGenerator = new KryoHashGenerator

  var txChainService: TransactionChainService[IO] = _
  var semaphore: Semaphore[IO] = _

  before {
    txChainService = TransactionChainService[IO]
    semaphore = Semaphore[IO](1).unsafeRunSync()
  }

  "update" - {
    "it should update existing transaction" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx = mock[Transaction]
      tx.hash shouldReturn "lorem"

      val tx1 = mock[TransactionCacheData]
      tx1.transaction shouldReturn tx
      tx1.hash shouldReturn "lorem"

      val tx2 = mock[TransactionCacheData]
      tx2.transaction shouldReturn tx
      tx2.hash shouldReturn "lorem"

      memPool.put("lorem", tx1).unsafeRunSync

      memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx1)

      memPool.update("lorem", _ => tx2).unsafeRunSync

      memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx2)
    }

    "it should not update transaction if it does not exist" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx = mock[Transaction]
      tx.hash shouldReturn "lorem"

      val tx1 = mock[TransactionCacheData]
      tx1.transaction shouldReturn tx

      val tx2 = mock[TransactionCacheData]
      tx2.transaction shouldReturn tx

      memPool.lookup("lorem").unsafeRunSync shouldBe None

      memPool.update("lorem", _ => tx2).unsafeRunSync

      memPool.lookup("lorem").unsafeRunSync shouldBe None
    }
  }

  "pull" - {
    "it should return None if there are less txs than min required count" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      memPool.pull(10).unsafeRunSync shouldBe none
    }

    "it should return min required count of txs" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx1 = createTransaction("a")
      val tx2 = createTransaction("b")
      val tx3 = createTransaction("c")

      (memPool.put("a", tx1) >>
        memPool.put("b", tx2) >>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx2, tx1).some
    }

    "it should return transactions sorted by the fee" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx1 = createTransaction("a", fee = 3L.some)
      val tx2 = createTransaction("b", fee = 1L.some)
      val tx3 = createTransaction("c", fee = 5L.some)

      (memPool.put("a", tx1) >>
        memPool.put("b", tx2) >>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx3, tx1).some
    }

    "it should return transactions sorted by the address and the fee" in {
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val txs = List(
        createTransaction("a", fee = 3L.some),
        createTransaction("a", fee = 3L.some),
        createTransaction("a", fee = 4L.some),
        createTransaction("a", fee = 2L.some),
        createTransaction("b", fee = 3L.some),
        createTransaction("b", fee = 2L.some),
        createTransaction("b", fee = 1L.some),
        createTransaction("c", fee = 10L.some),
        createTransaction("c", fee = 3L.some)
      )

      txs.traverse(tx => memPool.put(tx.hash, tx)).unsafeRunSync()

      memPool
        .pull(10)
        .map(_.get.map(t => (t.transaction.src.address, t.transaction.lastTxRef.ordinal)))
        .unsafeRunSync shouldBe List(
        ("c", 0),
        ("c", 1),
        ("a", 0),
        ("a", 1),
        ("a", 2),
        ("a", 3),
        ("b", 0),
        ("b", 1),
        ("b", 2)
      )
    }
  }

  def createTransaction(
    src: String,
    fee: Option[Long] = None
  ): TransactionCacheData = {
    import constellation._

    val txData = TransactionEdgeData(1L, fee = fee)

    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash("dst", EdgeHashType.AddressHash)),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(Fixtures.tempKey, Fixtures.hashGenerator)

    txChainService.setLastTransaction(Edge(oe, soe, txData), false).map(TransactionCacheData(_)).unsafeRunSync
  }

}
