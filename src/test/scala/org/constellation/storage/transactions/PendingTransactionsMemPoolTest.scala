package org.constellation.storage.transactions

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.ConstellationContextShift
import org.constellation.primitives.Schema.TransactionEdgeData
import org.constellation.primitives.{Edge, Transaction, TransactionCacheData}
import org.mockito.IdiomaticMockito
import org.scalatest.{FreeSpec, FunSuite, Matchers}

class PendingTransactionsMemPoolTest extends FreeSpec with IdiomaticMockito with Matchers {
  implicit val cs: ContextShift[IO] = ConstellationContextShift.global

  "update" - {
    "it should update existing transaction" in {
      val semaphore = Semaphore[IO](1).unsafeRunSync()
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
      val semaphore = Semaphore[IO](1).unsafeRunSync()
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
      val semaphore = Semaphore[IO](1).unsafeRunSync()
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      memPool.pull(10).unsafeRunSync shouldBe none
    }

    "it should return min required count of txs" in {
      val semaphore = Semaphore[IO](1).unsafeRunSync()
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx1 = createTransaction("a")
      val tx2 = createTransaction("b")
      val tx3 = createTransaction("c")

      (memPool.put("a", tx1) *>
        memPool.put("b", tx2) *>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx1, tx2).some
    }

    "it should return transactions sorted by the fee" in {
      val semaphore = Semaphore[IO](1).unsafeRunSync()
      val memPool = new PendingTransactionsMemPool[IO](semaphore)

      val tx1 = createTransaction("a", fee = 3L.some)
      val tx2 = createTransaction("b", fee = 1L.some)
      val tx3 = createTransaction("c", fee = 5L.some)

      (memPool.put("a", tx1) *>
        memPool.put("b", tx2) *>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx3, tx1).some
    }
  }

  private def createTransaction(hash: String, fee: Option[Long] = None): TransactionCacheData = {
    val tx = mock[TransactionCacheData]

    tx.transaction shouldReturn mock[Transaction]
    tx.transaction.hash shouldReturn hash
    tx.transaction.edge shouldReturn mock[Edge[TransactionEdgeData]]
    tx.transaction.edge.data shouldReturn mock[TransactionEdgeData]
    tx.transaction.edge.data.fee shouldReturn fee

    tx
  }

}
