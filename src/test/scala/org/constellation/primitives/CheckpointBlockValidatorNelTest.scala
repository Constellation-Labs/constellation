package org.constellation.primitives

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import cats.effect.IO
import cats.implicits._
import constellation.createTransaction
import org.constellation.consensus.{RandomData, Snapshot, SnapshotInfo}
import org.constellation.primitives.CheckpointBlockValidatorNel._
import org.constellation.primitives.Schema.{AddressCacheData, CheckpointCache, Height, Id}
import org.constellation.storage.{
  CheckpointBlocksMemPool,
  CheckpointService
, TransactionService}
import org.constellation.util.{HashSignature, Metrics}
import org.constellation.{DAO, NodeConfig}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{mock => _, _}

class CheckpointBlockValidatorNelTest
    extends FunSuite
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Matchers
    with BeforeAndAfter {

  implicit val dao: DAO = mock[DAO]

  val snapService: ThreadSafeSnapshotService = mock[ThreadSafeSnapshotService]
  val checkpointService: CheckpointService[IO] = mock[CheckpointService[IO]]

  val leftBlock: CheckpointBlock = mock[CheckpointBlock]
  val leftParent: CheckpointBlock = mock[CheckpointBlock]

  val rightBlock: CheckpointBlock = mock[CheckpointBlock]
  val rightParent: CheckpointBlock = mock[CheckpointBlock]

  val tx1: Transaction = mock[Transaction]
  tx1.hash shouldReturn "tx1"
  val tx2: Transaction = mock[Transaction]
  tx2.hash shouldReturn "tx2"
  val tx3: Transaction = mock[Transaction]
  tx3.hash shouldReturn "tx3"
  val tx4: Transaction = mock[Transaction]


  before {
    leftBlock.baseHash shouldReturn "block1"
    leftParent.baseHash shouldReturn "leftParent"

    rightBlock.baseHash shouldReturn "block2"
    rightParent.baseHash shouldReturn "rightParent"

    rightBlock.signatures shouldReturn Seq(HashSignature.apply("sig1", Id("id1")),
                                           HashSignature.apply("sig2", Id("id2")))
    leftBlock.signatures shouldReturn Seq(HashSignature.apply("sig1", Id("id1")),
                                          HashSignature.apply("sig2", Id("id2")))

    rightBlock.parentSOEBaseHashes()(*) shouldReturn Seq("rightParent")
    leftBlock.parentSOEBaseHashes()(*) shouldReturn Seq("leftParent")

    leftParent.parentSOEBaseHashes()(*) shouldReturn Seq.empty
    rightParent.parentSOEBaseHashes()(*) shouldReturn Seq.empty

    leftParent.transactions shouldReturn Seq.empty
    rightParent.transactions shouldReturn Seq.empty

    checkpointService.fullData(rightParent.baseHash) shouldReturn IO.pure(Some(CheckpointCache(Some(rightParent))))
    checkpointService.fullData(leftParent.baseHash) shouldReturn IO.pure(Some(CheckpointCache(Some(leftParent))))
    checkpointService.memPool shouldReturn mock[CheckpointBlocksMemPool[IO]]
    checkpointService.memPool.size() shouldReturn IO.pure(0)
    dao.transactionService shouldReturn mock[TransactionService[IO]]
    dao.transactionService.isAccepted(*) shouldReturn IO.pure(false)

    leftBlock.transactions shouldReturn Seq(tx1, tx2)
    rightBlock.transactions shouldReturn Seq(tx3, tx4)

    dao.threadSafeSnapshotService shouldReturn snapService
    dao.checkpointService shouldReturn checkpointService

    val metrics = mock[Metrics]
    dao.metrics shouldReturn metrics

    val cbNotInSnapshot = Seq(leftBlock.baseHash, rightBlock.baseHash, leftParent.baseHash, rightParent.baseHash)
    snapService.acceptedCBSinceSnapshot shouldReturn cbNotInSnapshot
  }

  test("it should detect no internal conflict and return None") {
    detectInternalTipsConflict(
      Seq(CheckpointCache(Some(leftBlock)), CheckpointCache(Some(rightBlock)))
    ) shouldBe None
  }

  test("it should detect no conflict and return None") {
    containsAlreadyAcceptedTx(leftBlock).unsafeRunSync() shouldBe List.empty
  }

  test("it should detect direct internal conflict with other tip") {
    val rightBlockTx = rightBlock.transactions.head
    leftBlock.transactions shouldReturn Seq(tx1, tx2, rightBlockTx)

    detectInternalTipsConflict(
      Seq(CheckpointCache(Some(leftBlock)), CheckpointCache(Some(rightBlock)))
    ) shouldBe Some(CheckpointCache(Some(rightBlock)))
  }
  test("it should detect direct conflict with other tip") {
    val rightBlockTx = rightBlock.transactions.head
    leftBlock.transactions shouldReturn Seq(tx1, tx2, rightBlockTx)

    detectInternalTipsConflict(
      Seq(CheckpointCache(Some(leftBlock)), CheckpointCache(Some(rightBlock)))
    ) shouldBe Some(CheckpointCache(Some(rightBlock)))
  }

  test("it should detect conflict with ancestry of other tip") {
    val conflictTx = leftBlock.transactions.head.hash
    dao.transactionService.isAccepted(conflictTx) shouldReturn IO.pure(true)


    containsAlreadyAcceptedTx(leftBlock).unsafeRunSync() shouldBe List(conflictTx)
  }

  test("it should get transactions from parent") {
    rightParent.transactions shouldReturn Seq(tx2)
    val ancestors = Seq("ancestor_in_snap")
    rightParent.parentSOEBaseHashes() shouldReturn ancestors
    checkpointService.fullData("ancestor_in_snap") shouldReturn IO.pure(None)

    val combinedTxs =
      getTransactionsTillSnapshot(List(rightBlock))
    combinedTxs shouldBe  (rightBlock.transactions ++ rightParent.transactions).map(_.hash)
  }

  test("it should return false for cb not in snap") {
    isInSnapshot(rightParent) shouldBe false
    isInSnapshot(leftParent) shouldBe false
  }

  test("it should return correct block to preserve with greater base hash") {
    selectBlockToPreserve(
      Seq(CheckpointCache(Some(leftBlock)), CheckpointCache(Some(rightBlock)))
    ) shouldBe CheckpointCache(Some(rightBlock))
  }

  test("it should return correct block to preserve with greater number of signatures") {
    val signatures = rightBlock.signatures ++ Seq(HashSignature.apply("extraSig", Id("extra_id1")))

    leftBlock.signatures shouldReturn signatures

    selectBlockToPreserve(
      Seq(CheckpointCache(Some(leftBlock)), CheckpointCache(Some(rightBlock)))
    ) shouldBe CheckpointCache(Some(leftBlock))
  }

  test("it should return correct block to preserve with greater number of children") {
    selectBlockToPreserve(
      Seq(CheckpointCache(Some(leftBlock), 2), CheckpointCache(Some(rightBlock)))
    ) shouldBe CheckpointCache(Some(leftBlock), 2)
  }
}

class ValidationSpec
  extends TestKit(ActorSystem("Validation"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockFactory
    with OneInstancePerTest {

  import RandomData._

  implicit val dao: DAO = new DAO()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  dao.initialize(NodeConfig())
  implicit val keyPair: KeyPair = keyPairs.head
  dao.metrics = new Metrics()
  val peerProbe = TestProbe.apply("peerManager")
  dao.peerManager = peerProbe.ref

  go.genesis.store(CheckpointCache(Some(go.genesis)))
  go.initialDistribution.store(CheckpointCache(Some(go.initialDistribution)))
  go.initialDistribution2.store(CheckpointCache(Some(go.initialDistribution2)))
  dao.threadSafeSnapshotService.setSnapshot(
    SnapshotInfo(
      Snapshot.snapshotZero,
      Seq(go.genesis.baseHash, go.initialDistribution.baseHash, go.initialDistribution2.baseHash),
      Seq(),
      0,
      Seq(),
      Map.empty,
      Map.empty,
      Seq()
    )
  )

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Checkpoint block validation" when {
    "all transactions are valid" should {
      "pass validation" in {
        val kp = keyPairs.take(6)
        val _ :: a :: b :: c :: d :: e :: _ = kp

        val txs1 = fill(
          Map(
            getAddress(a) -> 150L,
            getAddress(b) -> 0L,
            getAddress(c) -> 150L
          )
        )

        val txs2 = fill(
          Map(
            getAddress(d) -> 15L,
            getAddress(e) -> 0L
          )
        )

        val cbInit1 =
          CheckpointBlock.createCheckpointBlockSOE(txs1.toSeq, startingTips)
        val cbInit2 =
          CheckpointBlock.createCheckpointBlockSOE(txs2.toSeq, startingTips)

        cbInit1.store(CheckpointCache(Some(cbInit1)))
        cbInit2.store(CheckpointCache(Some(cbInit2)))

        setupSnapshot(Seq(cbInit1, cbInit2))

        val tips = Seq(cbInit1.soe, cbInit2.soe)

        // First group
        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), tips)

        val tx2 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), tips)

        val tx3 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb3 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx3), Seq(cb1.soe, cb2.soe))

        // Second group
        val tx4 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb4 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx4), tips)

        val tx5 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb5 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx5), tips)

        val tx6 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb6 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx6), Seq(cb4.soe, cb5.soe))

        // Tip
        val tx7 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb7 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx7), Seq(cb3.soe, cb6.soe))

        Seq(cb1, cb2, cb3, cb4, cb5, cb6, cb7)
          .foreach(cb => cb.store(CheckpointCache(Some(cb))))

        println(dao.metrics)
        assert(cb7.simpleValidation())
      }
    }

    "block is malformed" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(c), getAddress(b), 75L, c)
        )

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)

        fill(
          Map(
            getAddress(a) -> 74L,
            getAddress(c) -> 75L
          )
        )

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction is duplicated" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val tx = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val txs = Seq(tx, tx)

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)

        fill(
          Map(
            getAddress(a) -> 150L
          )
        )

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction has non-positive amount" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(b), getAddress(c), -5L, b)
        )

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)

        fill(
          Map(
            getAddress(a) -> 75L,
            getAddress(b) -> 75L
          )
        )

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction has zero amount" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(b), getAddress(c), 0L, b)
        )

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

        fill(
          Map(
            getAddress(a) -> 75L,
            getAddress(b) -> 75L
          )
        )

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction has no address cache stored" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(a), getAddress(c), 75L, a),
        )

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)

        assert(!cb.simpleValidation())
      }
    }

    "checkpoint block is internally inconsistent" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(a), getAddress(c), 75L, a),
        )

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)

        fill(
          Map(
            getAddress(a) -> 100L
          )
        )

        assert(!cb.simpleValidation())
      }
    }
  }

  "two checkpoint blocks has same ancestor" when {
    "combined relative to the snapshot are invalid" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        fill(
          Map(
            getAddress(a) -> 100L,
            getAddress(b) -> 0L,
            getAddress(c) -> 0L
          )
        )

        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), startingTips)

        val tx2 = createTransaction(getAddress(a), getAddress(c), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), startingTips)

        if (cb1.simpleValidation()) {
          cb1.transactions.toList
            .map(dao.addressService.transfer)
            .sequence[IO, AddressCacheData]
            .unsafeRunSync()
        }

        assert(!cb2.simpleValidation())
      }
    }
  }

  "two groups of checkpoint blocks lower blocks beyond the snapshot" when {
    "first group is internally inconsistent" should {
      "not pass validation" in {
        val kp = keyPairs.take(6)
        val _ :: a :: b :: c :: d :: e :: _ = kp

        val txs1 = fill(
          Map(
            getAddress(a) -> 100L,
            getAddress(b) -> 0L,
            getAddress(c) -> 150L
          )
        )

        val txs2 = fill(
          Map(
            getAddress(d) -> 15L,
            getAddress(e) -> 0L
          )
        )

        val cbInit1 =
          CheckpointBlock.createCheckpointBlockSOE(txs1.toSeq, startingTips)
        val cbInit2 =
          CheckpointBlock.createCheckpointBlockSOE(txs2.toSeq, startingTips)

        cbInit1.store(CheckpointCache(Some(cbInit1)))
        cbInit2.store(CheckpointCache(Some(cbInit2)))

        setupSnapshot(Seq(cbInit1, cbInit2))

        val tips = Seq(cbInit1.soe, cbInit2.soe)

        // First group
        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), tips)

        val tx2 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), tips)

        val tx3 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb3 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx3), Seq(cb1.soe, cb2.soe))

        // Second group
        val tx4 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb4 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx4), tips)

        val tx5 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb5 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx5), tips)

        val tx6 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb6 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx6), Seq(cb4.soe, cb5.soe))

        // Tip
        val tx7 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb7 =
          CheckpointBlock.createCheckpointBlockSOE(Seq(tx7), Seq(cb3.soe, cb6.soe))

        Seq(cb1, cb2, cb3, cb4, cb5, cb6, cb7)
          .foreach { cb =>
            dao.threadSafeSnapshotService
              .accept(CheckpointCache(Some(cb), 0, Some(Height(1, 2))))
              .unsafeRunSync()
          }

        assert(!cb7.simpleValidation())
      }
    }
  }
}
