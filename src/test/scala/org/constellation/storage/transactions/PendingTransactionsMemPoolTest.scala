//package org.constellation.storage.transactions
//
//import cats.effect.{ContextShift, IO}
//import cats.syntax.all._
//import org.constellation.domain.transaction.{PendingTransactionsMemPool, TransactionChainService, TransactionService}
//import org.constellation.schema.edge.{Edge, EdgeHashType, ObservationEdge, TypedEdgeHash}
//import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionCacheData, TransactionEdgeData}
//import org.constellation.serialization.KryoSerializer
//import org.constellation.storage.RateLimiting
//import org.constellation.{DAO, Fixtures, TestHelpers}
//import org.mockito.IdiomaticMockito
//import org.scalatest.BeforeAndAfter
//import org.scalatest.freespec.AnyFreeSpec
//import org.scalatest.matchers.should.Matchers
//
//import scala.concurrent.ExecutionContext
//
//class PendingTransactionsMemPoolTest extends AnyFreeSpec with IdiomaticMockito with Matchers with BeforeAndAfter {
//  import constellation.signedObservationEdge
//  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
//
//  var txChainService: TransactionChainService[IO] = _
//  var txService: TransactionService[IO] = _
//  var rl: RateLimiting[IO] = _
//  var dao: DAO = _
//
//  before {
//    KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()
//    dao = TestHelpers.prepareMockedDAO()
//    txChainService = TransactionChainService[IO]
//    rl = RateLimiting[IO]()
//    txService = TransactionService[IO](txChainService, rl, dao)
//  }
//
//  "update" - {
//    "it should update existing transaction" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx = mock[Transaction]
//      tx.hash shouldReturn "lorem"
//
//      val tx1 = mock[TransactionCacheData]
//      tx1.transaction shouldReturn tx
//      tx1.hash shouldReturn "lorem"
//
//      val tx2 = mock[TransactionCacheData]
//      tx2.transaction shouldReturn tx
//      tx2.hash shouldReturn "lorem"
//
//      memPool.put("lorem", tx1).unsafeRunSync
//
//      memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx1)
//
//      memPool.update("lorem", _ => tx2).unsafeRunSync
//
//      memPool.lookup("lorem").unsafeRunSync shouldBe Some(tx2)
//    }
//
//    "it should not update transaction if it does not exist" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx = mock[Transaction]
//      tx.hash shouldReturn "lorem"
//
//      val tx1 = mock[TransactionCacheData]
//      tx1.transaction shouldReturn tx
//
//      val tx2 = mock[TransactionCacheData]
//      tx2.transaction shouldReturn tx
//
//      memPool.lookup("lorem").unsafeRunSync shouldBe None
//
//      memPool.update("lorem", _ => tx2).unsafeRunSync
//
//      memPool.lookup("lorem").unsafeRunSync shouldBe None
//    }
//  }
//
//  "pull" - {
//    "it should return None if there are no transactions to return" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      memPool.pull(10).unsafeRunSync shouldBe none
//    }
//
//    "it should return up to max count of txs" in {
//      val memPool = new PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = createTransaction("a")
//      val tx2 = createTransaction("b")
//      val tx3 = createTransaction("c")
//
//      (memPool.put("a", tx1) >>
//        memPool.put("b", tx2) >>
//        memPool.put("c", tx3)).unsafeRunSync
//
//      memPool.pull(2).unsafeRunSync shouldBe List(tx2, tx1).some
//    }
//
//    "it should return transactions sorted by the fee" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = createTransaction("a", fee = 3L.some)
//      val tx2 = createTransaction("b", fee = 1L.some)
//      val tx3 = createTransaction("c", fee = 5L.some)
//
//      (memPool.put("a", tx1) >>
//        memPool.put("b", tx2) >>
//        memPool.put("c", tx3)).unsafeRunSync
//
//      memPool.pull(2).unsafeRunSync shouldBe List(tx3, tx1).some
//    }
//
//    "it should return transactions sorted by the address and the fee" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val txs = List(
//        createTransaction("a", fee = 3L.some),
//        createTransaction("a", fee = 3L.some),
//        createTransaction("a", fee = 4L.some),
//        createTransaction("a", fee = 2L.some),
//        createTransaction("b", fee = 3L.some),
//        createTransaction("b", fee = 2L.some),
//        createTransaction("b", fee = 1L.some),
//        createTransaction("c", fee = 10L.some),
//        createTransaction("c", fee = 3L.some)
//      )
//
//      txs.traverse(tx => memPool.put(tx.hash, tx)).unsafeRunSync
//
//      memPool
//        .pull(10)
//        .map(_.get.map(t => (t.transaction.src.address, t.transaction.lastTxRef.ordinal)))
//        .unsafeRunSync shouldBe List(
//        ("c", 0),
//        ("c", 1),
//        ("a", 0),
//        ("a", 1),
//        ("a", 2),
//        ("a", 3),
//        ("b", 0),
//        ("b", 1),
//        ("b", 2)
//      )
//    }
//
//    "it should not return transactions for which the latest reference was not accepted" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = createTransaction("a")
//      val tx2 = createTransaction("a")
//      val tx3 = createTransaction("a")
//
//      txService.accept(tx1).unsafeRunSync
//
//      memPool.put(tx3.hash, tx3).unsafeRunSync
//
//      val ret = memPool.pull(10).unsafeRunSync
//
//      ret shouldBe None
//    }
//
//    "it should return None when no transaction references last accepted transaction" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = manuallyCreateTransaction("a", "", 1)
//      val tx2 = manuallyCreateTransaction("a", "abc", 0)
//
//      (memPool.put(tx1.hash, tx1) >>
//        memPool.put(tx2.hash, tx2)).unsafeRunSync
//
//      val result = memPool.pull(10).unsafeRunSync
//
//      result shouldBe None
//    }
//
//    "it should return a consecutive chain of transactions if first tx in a chain references last accepted tx" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = manuallyCreateTransaction("a", "", 0)
//      val tx2 = manuallyCreateTransaction("a", tx1.transaction.hash, tx1.transaction.ordinal)
//      val tx3 = manuallyCreateTransaction("a", tx2.transaction.hash, tx2.transaction.ordinal)
//      val tx4 = manuallyCreateTransaction("a", tx3.transaction.hash, tx3.transaction.ordinal)
//
//      //not sending tx3 to pending to break the chain
//      (memPool.put(tx1.hash, tx1) >>
//        memPool.put(tx2.hash, tx2) >>
//        memPool.put(tx4.hash, tx4)).unsafeRunSync
//
//      val result = memPool.pull(10).unsafeRunSync
//
//      result shouldBe Some(List(tx1, tx2))
//    }
//
//    "if there are transactions referring to the same ordinal but different hash or other way around - should pick the correct one if exists" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = manuallyCreateTransaction("a", "", 0)
//      val tx2Correct = manuallyCreateTransaction("a", tx1.transaction.hash, tx1.transaction.ordinal)
//      val tx2WrongOrdinal = manuallyCreateTransaction("a", tx1.transaction.hash, 2)
//      val tx2WrongHash = manuallyCreateTransaction("a", "wrongHash", tx1.transaction.ordinal)
//
//      (memPool.put(tx1.hash, tx1) >>
//        memPool.put(tx2Correct.hash, tx2Correct) >>
//        memPool.put(tx2WrongOrdinal.hash, tx2WrongOrdinal) >>
//        memPool.put(tx2WrongHash.hash, tx2WrongHash)).unsafeRunSync
//
//      val result = memPool.pull(10).unsafeRunSync
//
//      result shouldBe Some(List(tx1, tx2Correct))
//    }
//
//    "it should not return transactions from addresses which are over the limit" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val tx1 = createTransaction("a")
//      val tx2 = createTransaction("a")
//
//      (rl.update(List(tx1).map(_.transaction)) >>
//        txService.accept(tx1) >>
//        memPool.put(tx2.hash, tx2)).unsafeRunSync
//
//      val result = memPool.pull(10).unsafeRunSync
//
//      result shouldBe None
//    }
//
//    "it should return transactions with positive fee from addresses that are over the limit" in {
//      val memPool = PendingTransactionsMemPool[IO](txChainService, rl)
//
//      val txa1 = createTransaction("a")
//      val txa2 = createTransaction("a", Some(2L))
//      val txa3 = createTransaction("a", Some(0L))
//      val txb1 = createTransaction("b")
//      val txb2 = createTransaction("b", Some(1L))
//      val txb3 = createTransaction("b")
//
//      (rl.update(List(txa1, txb1).map(_.transaction)) >>
//        txService.accept(txa1) >>
//        txService.accept(txb1) >>
//        memPool.put(txa2.hash, txa2) >>
//        memPool.put(txa3.hash, txa3) >>
//        memPool.put(txb2.hash, txb2) >>
//        memPool.put(txb3.hash, txb3)).unsafeRunSync
//
//      val result = memPool.pull(10).unsafeRunSync
//
//      result shouldBe List(txa2, txb2).some
//    }
//  }
//
//  def createTransaction(
//    src: String,
//    fee: Option[Long] = None
//  ): TransactionCacheData =
//    txChainService
//      .createAndSetLastTransaction(src, "dst", 1L, Fixtures.tempKey, false, fee)
//      .map(TransactionCacheData(_))
//      .unsafeRunSync
//
//  def manuallyCreateTransaction(src: String, lastTxHash: String, lastTxOrdinal: Long): TransactionCacheData = {
//    val lastTxRef = LastTransactionRef(lastTxHash, lastTxOrdinal)
//    val data = TransactionEdgeData(1L, lastTxRef)
//    val oe = ObservationEdge(
//      parents = Seq(
//        TypedEdgeHash(src, EdgeHashType.AddressHash),
//        TypedEdgeHash("dst", EdgeHashType.AddressHash)
//      ),
//      data = TypedEdgeHash(data.hash, EdgeHashType.TransactionDataHash)
//    )
//    val soe = signedObservationEdge(oe)(Fixtures.tempKey)
//
//    TransactionCacheData(
//      Transaction(Edge(oe, soe, data), lastTxRef)
//    )
//  }
//}
