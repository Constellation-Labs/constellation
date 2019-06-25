package org.constellation.storage.transactions

import cats.effect.IO
import org.constellation.primitives.{Transaction, TransactionCacheData}
import org.mockito.IdiomaticMockito
import org.scalatest.{FunSuite, Matchers}

class PendingTransactionsMemPoolTest extends FunSuite with IdiomaticMockito with Matchers {
  test("it should update existing transaction") {
    val memPool = new PendingTransactionsMemPool[IO]

    val tx = mock[Transaction]
    tx.hash shouldReturn "lorem"

    val tx1 = mock[TransactionCacheData]
    tx1.transaction shouldReturn tx

    val tx2 = mock[TransactionCacheData]
    tx2.transaction shouldReturn tx

    memPool.put("lorem", tx1).unsafeRunSync

    memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx1)

    memPool.update("lorem", _ => tx2).unsafeRunSync

    memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx2)
  }

  test("it should not update existing transaction") {
    val memPool = new PendingTransactionsMemPool[IO]

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
