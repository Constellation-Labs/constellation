package org.constellation.consensus

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.Metrics
import org.constellation.{DAO, Fixtures}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.concurrent.TrieMap
import scala.util.Random

object RandomData {

  val logger = Logger("RandomData")
  
  val keyPairs: Seq[KeyPair] = Seq.fill(10)(makeKeyPair())

  val go: GenesisObservation = Genesis.createGenesisAndInitialDistributionDirect(
    keyPairs.head.address.address,
    keyPairs.tail.map {
      _.getPublic.toId
    }.toSet,
    keyPairs.head
  )

  val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  def randomBlock(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction)
    CheckpointBlock.createCheckpointBlock(txs, tips.map{s => TypedEdgeHash(s.hash, EdgeHashType.CheckpointHash)})(startingKeyPair)
  }

  def randomTransaction: Transaction = {
    val src = Random.shuffle(keyPairs).head
    createTransaction(
      src.address.address,
      Random.shuffle(keyPairs).head.address.address,
      Random.nextInt(500).toLong,
      src
    )
  }

  def getAddress(keyPair: KeyPair): String = keyPair.address.address

  def fill(balances: Map[String, Long])(implicit dao: DAO): Iterable[Transaction] = {
    val txs = balances.map {
      case (address, amount) => createTransaction(keyPairs.head.address.address, address, amount, keyPairs.head)
    }

    txs.foreach(tx => {
      tx.ledgerApply()
      tx.ledgerApplySnapshot()
    })

    txs
  }

  def setupSnapshot(cb: Seq[CheckpointBlock])(implicit dao: DAO): Seq[CheckpointBlock] = {
    dao.threadSafeTipService.setSnapshot(SnapshotInfo(
      dao.threadSafeTipService.getSnapshotInfo().snapshot,
      cb.map(_.baseHash),
      Seq(),
      1,
      Seq(),
      Map.empty,
      Map.empty,
      Seq()
    ))

    cb
  }
}

class RandomDataTest extends FlatSpec {

  import RandomData._

  "Signatures combiners" should "be unique" in {

    val cb = randomBlock(startingTips, keyPairs.head)
    val cb2SameSignature = randomBlock(startingTips, keyPairs.head)

    //    val bogus = cb.plus(keyPairs.head).signatures
    //    bogus.foreach{logger.debug}

    logger.debug(hashSign("asdf", keyPairs.head).toString)
    logger.debug(hashSign("asdf", keyPairs.head).toString)
    logger.debug(hashSign("asdf", keyPairs.head).toString)
    logger.debug(hashSign("asdf", keyPairs.head).toString)
    logger.debug(hashSign("asdf", keyPairs.head).toString)

    //    assert(bogus.size == 1)

    //val cb = randomBlock(startingTips, keyPairs.last)
  }

  "Generate random CBs" should "build a graph" in {

    var width = 2
    val maxWidth = 50


    val activeBlocks = TrieMap[SignedObservationEdge, Int]()


    val maxNumBlocks = 1000
    var blockNum = 0

    activeBlocks(startingTips.head) = 0
    activeBlocks(startingTips.last) = 0

    val cbIndex = TrieMap[SignedObservationEdge, CheckpointBlock]()
    //cbIndex(go.initialDistribution.soe) = go.initialDistribution
    //cbIndex(go.initialDistribution2.soe) = go.initialDistribution2
    var blockId = 3

    val convMap = TrieMap[String, Int]()


    val snapshotInterval = 10

    while (blockNum < maxNumBlocks) {

      blockNum += 1
      val tips = Random.shuffle(activeBlocks).take(2)

      tips.foreach { case (tip, numUses) =>

        def doRemove(): Unit = activeBlocks.remove(tip)

        if (width < maxWidth) {
          if (numUses >= 2) {
            doRemove()
          } else {
            activeBlocks(tip) += 1
          }
        } else {
          doRemove()
        }

      }

      val block = randomBlock(tips.map {
        _._1
      }.toSeq)
      cbIndex(block.soe) = block
      activeBlocks(block.soe) = 0

      convMap(block.soe.hash) = blockId
      blockId += 1
      width = activeBlocks.size

      block


    }

    logger.debug(cbIndex.size.toString)

    val genIdMap = Map(
      go.genesis.soe.hash -> 0,
      go.initialDistribution.soe.hash -> 1,
      go.initialDistribution2.soe.hash -> 2
    )

    val conv = convMap.toMap ++ genIdMap /*cbIndex.toSeq.zipWithIndex.map{
      case ((soe, cb), id) =>
        soe.hash -> (id + 3)
    }.toMap*/

    val json = (cbIndex.toSeq.map {
      case (soe, cb) =>
        Map("id" -> conv(soe.hash), "parentIds" -> cb.parentSOEHashes.map {
          conv
        })
    } ++ Seq(
      Map("id" -> conv(go.initialDistribution.soe.hash), "parentIds" -> Seq(conv(go.genesis.soe.hash))),
      Map("id" -> conv(go.initialDistribution2.soe.hash), "parentIds" -> Seq(conv(go.genesis.soe.hash))),
      Map("id" -> conv(go.genesis.soe.hash), "parentIds" -> Seq[String]())
    )).json
    logger.debug(json)

    //  file"../d3-dag/test/data/dag.json".write(json)


    //createTransaction()


  }

}

class ValidationSpec extends TestKit(ActorSystem("Validation")) with WordSpecLike with Matchers with BeforeAndAfterEach
  with BeforeAndAfterAll with MockFactory with OneInstancePerTest {

  import RandomData._

  implicit val dao: DAO = stub[DAO]
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  (dao.id _).when().returns(Fixtures.id)

  dao.keyPair = KeyUtils.makeKeyPair()
  dao.metrics = new Metrics()
  val peerProbe = TestProbe.apply("peerManager")
  dao.peerManager = peerProbe.ref

  go.genesis.store(CheckpointCacheData(Some(go.genesis)))
  go.initialDistribution.store(CheckpointCacheData(Some(go.initialDistribution)))
  go.initialDistribution2.store(CheckpointCacheData(Some(go.initialDistribution2)))
  dao.threadSafeTipService.setSnapshot(SnapshotInfo(
    Snapshot.snapshotZero,
    Seq(go.genesis.baseHash, go.initialDistribution.baseHash, go.initialDistribution2.baseHash),
    Seq(),
    0,
    Seq(),
    Map.empty,
    Map.empty,
    Seq()
  ))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Checkpoint block validation" when {
    "all transactions are valid" should {
      "pass validation" in {
        val kp = keyPairs.take(6)
        val _ :: a :: b :: c :: d :: e :: _ = kp

        val txs1 = fill(Map(
          getAddress(a) -> 150L,
          getAddress(b) -> 0L,
          getAddress(c) -> 150L
        ))

        val txs2 = fill(Map(
          getAddress(d) -> 15L,
          getAddress(e) -> 0L
        ))

        val cbInit1 = CheckpointBlock.createCheckpointBlockSOE(txs1.toSeq, startingTips)(keyPairs.head)
        val cbInit2 = CheckpointBlock.createCheckpointBlockSOE(txs2.toSeq, startingTips)(keyPairs.head)

        cbInit1.store(CheckpointCacheData(Some(cbInit1)))
        cbInit2.store(CheckpointCacheData(Some(cbInit2)))

        setupSnapshot(Seq(cbInit1, cbInit2))

        val tips = Seq(cbInit1.soe, cbInit2.soe)

        // First group
        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), tips)(keyPairs.head)

        val tx2 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), tips)(keyPairs.head)

        val tx3 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb3 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx3), Seq(cb1.soe, cb2.soe))(keyPairs.head)

        // Second group
        val tx4 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb4 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx4), tips)(keyPairs.head)

        val tx5 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb5 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx5), tips)(keyPairs.head)

        val tx6 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb6 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx6), Seq(cb4.soe, cb5.soe))(keyPairs.head)

        // Tip
        val tx7 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb7 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx7), Seq(cb3.soe, cb6.soe))(keyPairs.head)

        Seq(cb1, cb2, cb3, cb4, cb5, cb6, cb7)
          .foreach(cb => cb.store(CheckpointCacheData(Some(cb))))

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

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

        fill(Map(
          getAddress(a) -> 74L,
          getAddress(c) -> 75L
        ))

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction is duplicated" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        val tx = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val txs = Seq(tx, tx)

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

        fill(Map(
          getAddress(a) -> 150L
        ))

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

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

        fill(Map(
          getAddress(a) -> 75L,
          getAddress(b) -> 75L
        ))

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

        fill(Map(
          getAddress(a) -> 75L,
          getAddress(b) -> 75L
        ))

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

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

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

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips)(keyPairs.head)

        fill(Map(
          getAddress(a) -> 100L
        ))

        assert(!cb.simpleValidation())
      }
    }
  }

  "two checkpoint blocks has same ancestor" when {
    "combined relative to the snapshot are invalid" should {
      "not pass validation" in {
        val kp = keyPairs.take(4)
        val _ :: a :: b :: c :: _ = kp

        fill(Map(
          getAddress(a) -> 100L,
          getAddress(b) -> 0L,
          getAddress(c) -> 0L
        ))

        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), startingTips)(keyPairs.head)

        val tx2 = createTransaction(getAddress(a), getAddress(c), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), startingTips)(keyPairs.head)

        if (cb1.simpleValidation()) { cb1.transactions.foreach(_.ledgerApply) }

        assert(!cb2.simpleValidation())
      }
    }
  }

  "two groups of checkpoint blocks lower blocks beyond the snapshot" when {
    "first group is internally inconsistent" should {
      "not pass validation" in {
        val kp = keyPairs.take(6)
        val _ :: a :: b :: c :: d :: e :: _ = kp

        val txs1 = fill(Map(
          getAddress(a) -> 100L,
          getAddress(b) -> 0L,
          getAddress(c) -> 150L
        ))

        val txs2 = fill(Map(
          getAddress(d) -> 15L,
          getAddress(e) -> 0L
        ))

        val cbInit1 = CheckpointBlock.createCheckpointBlockSOE(txs1.toSeq, startingTips)(keyPairs.head)
        val cbInit2 = CheckpointBlock.createCheckpointBlockSOE(txs2.toSeq, startingTips)(keyPairs.head)

        cbInit1.store(CheckpointCacheData(Some(cbInit1)))
        cbInit2.store(CheckpointCacheData(Some(cbInit2)))

        setupSnapshot(Seq(cbInit1, cbInit2))

        val tips = Seq(cbInit1.soe, cbInit2.soe)

        // First group
        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx1), tips)(keyPairs.head)

        val tx2 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb2 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx2), tips)(keyPairs.head)

        val tx3 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb3 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx3), Seq(cb1.soe, cb2.soe))(keyPairs.head)

        // Second group
        val tx4 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb4 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx4), tips)(keyPairs.head)

        val tx5 = createTransaction(getAddress(c), getAddress(a), 75L, c)
        val cb5 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx5), tips)(keyPairs.head)

        val tx6 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb6 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx6), Seq(cb4.soe, cb5.soe))(keyPairs.head)

        // Tip
        val tx7 = createTransaction(getAddress(d), getAddress(e), 5L, d)
        val cb7 = CheckpointBlock.createCheckpointBlockSOE(Seq(tx7), Seq(cb3.soe, cb6.soe))(keyPairs.head)

        Seq(cb1, cb2, cb3, cb4, cb5, cb6, cb7)
          .foreach { cb =>
            cb.store(CheckpointCacheData(Some(cb)))
            cb.storeSOE()
          }

        assert(!cb7.simpleValidation())
      }
    }
  }
}
