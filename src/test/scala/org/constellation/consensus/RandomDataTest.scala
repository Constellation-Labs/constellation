package org.constellation.consensus

import java.security.KeyPair

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import constellation._
import org.constellation.{DAO, Fixtures}
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.{EncodedPublicKey, Heartbeat}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.concurrent.TrieMap
import scala.util.Random

object RandomData {
  val keyPairs: Seq[KeyPair] = Seq.fill(10)(makeKeyPair())

  val go: GenesisObservation = Genesis.createGenesisAndInitialDistributionDirect(
    keyPairs.head.address.address,
    keyPairs.tail.map {
      _.getPublic.toId
    }.toSet,
    keyPairs.head
  )

  val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  def randomBlock(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): Schema.CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction)
    EdgeProcessor.createCheckpointBlock(txs, tips)(startingKeyPair)
  }

  def randomBlockWithDuplicatedTransaction(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction)
    val txsWithDuplication = txs :+ txs.head
    EdgeProcessor.createCheckpointBlock(txsWithDuplication, tips)(startingKeyPair)
  }

  def randomBlockWithInvalidTransactions(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction) :+ randomInvalidTransaction
    EdgeProcessor.createCheckpointBlock(txs, tips)(startingKeyPair)
  }

  def randomBlockWithEmptyTransaction(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction) :+ randomEmptyTransaction
    EdgeProcessor.createCheckpointBlock(txs, tips)(startingKeyPair)
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

  def randomInvalidTransaction: Transaction = {
    val src = Random.shuffle(keyPairs).head
    createTransaction(
      src.address.address,
      Random.shuffle(keyPairs).head.address.address,
      -100L,
      src
    )
  }

  def randomEmptyTransaction: Transaction = {
    val src = Random.shuffle(keyPairs).head
    createTransaction(
      src.address.address,
      Random.shuffle(keyPairs).head.address.address,
      0L,
      src
    )
  }

  def fillAddressCache(cb: CheckpointBlock, delta: Long)(implicit dao: DAO): Unit = {
    cb.transactions
      .map(t => t.src.address -> t.amount)
      .groupBy(_._1).mapValues(_.map(_._2).sum)
      .foreach { transaction =>
        dao.addressService.put(transaction._1, AddressCacheData(transaction._2 + delta, 0))
      }
  }

  def getAddress(keyPair: KeyPair): String = keyPair.address.address

  def fill(address: String, amount: Long)(implicit dao: DAO): Unit = {
    val tx = createTransaction(go.initialDistribution.soe.hash, address, amount, keyPairs.head)
    tx.ledgerApply
    tx.ledgerApplySnapshot
  }
}

class RandomDataTest extends FlatSpec {

  import RandomData._

  "Signatures combiners" should "be unique" in {

    val cb = randomBlock(startingTips, keyPairs.head)
    val cb2SameSignature = randomBlock(startingTips, keyPairs.head)

    //    val bogus = cb.plus(keyPairs.head).signatures
    //    bogus.foreach{println}

    println(hashSign("asdf", keyPairs.head))
    println(hashSign("asdf", keyPairs.head))
    println(hashSign("asdf", keyPairs.head))
    println(hashSign("asdf", keyPairs.head))
    println(hashSign("asdf", keyPairs.head))

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

    println(cbIndex.size)

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
    println(json)

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

  val hbProbe = TestProbe.apply("hearthbeat")
  dao.heartbeatActor = hbProbe.ref

  val metricsProbe = TestProbe.apply("metricsManager")
  dao.metricsManager = metricsProbe.ref

  val peerProbe = TestProbe.apply("peerManager")
  dao.peerManager = peerProbe.ref

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "Checkpoint block validation" when {
    "all transactions are valid" should {
      "pass validation" in {
        val cb = randomBlock(startingTips, keyPairs.head)

        fillAddressCache(cb, 0L)

        assert(cb.simpleValidation())
      }
    }

    "block is malformed" should {
      "not pass validation" in {
        val cb = randomBlock(startingTips, keyPairs.head)

        fillAddressCache(cb, -1L)

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction is duplicated" should {
      "not pass validation" in {
        val cb = randomBlockWithDuplicatedTransaction(startingTips, keyPairs.head)

        fillAddressCache(cb, 0L)

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction has non-positive amount" should {
      "not pass validation" in {
        val cb = randomBlockWithInvalidTransactions(startingTips, keyPairs.head)

        fillAddressCache(cb, 0L)

        assert(!cb.simpleValidation())
      }
    }

    "at least one transaction has zero amount" should {
      "not pass validation" in {
        val cb = randomBlockWithEmptyTransaction(startingTips, keyPairs.head)

        fillAddressCache(cb, 0L)

        assert(!cb.simpleValidation())
      }
    }

    "checkpoint block is internally inconsistent" should {
      "not pass validation" in {
        val kp = Random.shuffle(keyPairs).take(3)
        val a :: b :: c :: _ = kp

        val txs = Seq(
          createTransaction(getAddress(a), getAddress(b), 75L, a),
          createTransaction(getAddress(a), getAddress(c), 75L, a),
        )

        val cb = EdgeProcessor.createCheckpointBlock(txs, startingTips)(keyPairs.head)

        dao.addressService.put(getAddress(a), AddressCacheData(100L, 0))

        assert(!cb.simpleValidation())
      }
    }
  }

  "two checkpoint blocks has same ancestor" when {
    "combined relative to the snapshot are invalid" should {
      "not pass validation" in {
        val kp = Random.shuffle(keyPairs).take(3)
        val a :: b :: c :: _ = kp

        fill(getAddress(a), 100L)
        fill(getAddress(b), 0L)

        val tx1 = createTransaction(getAddress(a), getAddress(b), 75L, a)
        val cb1 = EdgeProcessor.createCheckpointBlock(Seq(tx1), startingTips)(keyPairs.head)

        val tx2 = createTransaction(getAddress(a), getAddress(c), 75L, a)
        val cb2 = EdgeProcessor.createCheckpointBlock(Seq(tx2), startingTips)(keyPairs.head)

        if (cb1.simpleValidation()) { cb1.transactions.foreach(_.ledgerApply) }

        assert(!cb2.simpleValidation())
      }
    }
  }
}
