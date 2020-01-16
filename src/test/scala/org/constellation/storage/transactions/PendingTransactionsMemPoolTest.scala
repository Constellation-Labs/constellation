package org.constellation.storage.transactions

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.domain.transaction.{PendingTransactionsMemPool, TransactionChainService, TransactionService}
import org.constellation.primitives.{Transaction, TransactionCacheData}
import org.constellation.storage.RateLimiting
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, TestHelpers}
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class PendingTransactionsMemPoolTest extends FreeSpec with IdiomaticMockito with Matchers with BeforeAndAfter {
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  var txChainService: TransactionChainService[IO] = _
  var txService: TransactionService[IO] = _
  var rl: RateLimiting[IO] = _
  var dao: DAO = _

  before {
    dao = TestHelpers.prepareMockedDAO()
    txChainService = TransactionChainService[IO]
    rl = RateLimiting[IO]()
    txService = TransactionService[IO](txChainService, rl, dao)
  }

  "update" - {
    "it should update existing transaction" in {
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

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
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

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
    "it should return None if there are no transactions to return" in {
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

      memPool.pull(10).unsafeRunSync shouldBe none
    }

    "it should return up to max count of txs" in {
      val memPool = new PendingTransactionsMemPool[IO](txChainService, rl)

      val tx1 = createTransaction("a")
      val tx2 = createTransaction("b")
      val tx3 = createTransaction("c")

      (memPool.put("a", tx1) >>
        memPool.put("b", tx2) >>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx2, tx1).some
    }

    "it should return transactions sorted by the fee" in {
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

      val tx1 = createTransaction("a", fee = 3L.some)
      val tx2 = createTransaction("b", fee = 1L.some)
      val tx3 = createTransaction("c", fee = 5L.some)

      (memPool.put("a", tx1) >>
        memPool.put("b", tx2) >>
        memPool.put("c", tx3)).unsafeRunSync

      memPool.pull(2).unsafeRunSync shouldBe List(tx3, tx1).some
    }

    "it should return transactions sorted by the address and the fee" in {
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

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

      txs.traverse(tx => memPool.put(tx.hash, tx)).unsafeRunSync

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

    "it should not return transactions for which the latest reference was not accepted" in {
      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)

      val tx1 = createTransaction("a")
      val tx2 = createTransaction("a")
      val tx3 = createTransaction("a")

      txService.accept(tx1).unsafeRunSync

      memPool.put(tx3.hash, tx3).unsafeRunSync

      val ret = memPool.pull(10).unsafeRunSync

      ret shouldBe None
    }
  }

  def createTransaction(
    src: String,
    fee: Option[Long] = None
  ): TransactionCacheData =
    txChainService
      .createAndSetLastTransaction(src, "dst", 1L, Fixtures.tempKey, false, fee)
      .map(TransactionCacheData(_))
      .unsafeRunSync

}
