//package org.constellation.consensus
//
//import java.security.KeyPair
//
//import cats.effect.IO
//import cats.implicits._
//import com.typesafe.scalalogging.Logger
//import constellation._
//import org.constellation.domain.snapshot.SnapshotInfo
//import org.constellation.keytool.KeyUtils._
//import org.constellation.primitives.Schema._
//import org.constellation.primitives._
//import org.constellation.rewards.EigenTrustAgents
//import org.constellation.{DAO, Fixtures, TestHelpers}
//import org.scalatest._
//
//import scala.collection.concurrent.TrieMap
//import scala.util.Random
//
//object RandomData {
//
//  val logger = Logger("RandomData")
//
//  val keyPairs: Seq[KeyPair] = Seq.fill(10)(makeKeyPair())
//
//  def go()(implicit dao: DAO): GenesisObservation = Genesis.createGenesisObservation(Seq.empty)
//
//  def startingTips(go: GenesisObservation)(implicit dao: DAO): Seq[SignedObservationEdge] =
//    Seq(go.initialDistribution.soe, go.initialDistribution2.soe)
//
//  def randomBlock(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
//    val txs = Seq.fill(5)(randomTransaction)
//    CheckpointBlock.createCheckpointBlock(txs, tips.map { s =>
//      TypedEdgeHash(s.hash, EdgeHashType.CheckpointHash)
//    })(startingKeyPair)
//  }
//
//  def randomTransaction: Transaction = {
//    val src = Random.shuffle(keyPairs).head
//    Fixtures.makeTransaction(
//      src.address,
//      Random.shuffle(keyPairs).head.address,
//      Random.nextInt(500).toLong,
//      src
//    )
//  }
//
//  def getAddress(keyPair: KeyPair): String = keyPair.address
//
//  def fill(balances: Map[String, Long])(implicit dao: DAO): Iterable[Transaction] = {
//    val txs = balances.map {
//      case (address, amount) =>
//        Fixtures.makeTransaction(keyPairs.head.address, address, amount, keyPairs.head)
//    }
//
//    txs.toList
//      .map(
//        tx ⇒
//          IO.pure(tx)
//            .flatTap(dao.addressService.transfer)
//            .flatTap(dao.addressService.transferSnapshot)
//      )
//      .sequence[IO, Transaction]
//      .unsafeRunSync()
//
//    txs
//  }
//
//  def setupSnapshot(cb: Seq[CheckpointBlock])(implicit dao: DAO): Seq[CheckpointBlock] = {
//    dao.snapshotService.getSnapshotInfo
//      .map(_.snapshot)
//      .flatMap { snapshot =>
//        dao.snapshotService.setSnapshot(
//          SnapshotInfo(
//            snapshot,
//            cb.map(_.baseHash),
//            Seq(),
//            Set(),
//            1,
//            Seq(),
//            Map.empty,
//            Map.empty,
//            Seq()
//          )
//        )
//      }
//      .unsafeRunSync()
//
//    cb
//  }
//}
//
//class RandomDataTest extends FlatSpec with BeforeAndAfter {
//
//  import RandomData._
//
//  implicit var dao: DAO = _
//
//  before {
//    dao = TestHelpers.prepareRealDao()
//  }
//
//  after {
//    dao.unsafeShutdown()
//  }
//
//  "Signatures combiners" should "be unique" in {
//
//    val genesis = go()
//    val cb = randomBlock(startingTips(genesis), keyPairs.head)
//    val cb2SameSignature = randomBlock(startingTips(genesis), keyPairs.head)
//
//    //    val bogus = cb.plus(keyPairs.head).signatures
//    //    bogus.foreach{logger.debug}
//
//    logger.debug(hashSign("asdf", keyPairs.head).toString)
//    logger.debug(hashSign("asdf", keyPairs.head).toString)
//    logger.debug(hashSign("asdf", keyPairs.head).toString)
//    logger.debug(hashSign("asdf", keyPairs.head).toString)
//    logger.debug(hashSign("asdf", keyPairs.head).toString)
//
//    //    assert(bogus.size == 1)
//
//    //val cb = randomBlock(startingTips, keyPairs.last)
//  }
//
//  "Generate random CBs" should "build a graph" in {
//
//    var width = 2
//    val maxWidth = 50
//
//    val activeBlocks = TrieMap[SignedObservationEdge, Int]()
//
//    val maxNumBlocks = 1000
//    var blockNum = 0
//
//    val genesis = RandomData.go()
//
//    activeBlocks(startingTips(genesis).head) = 0
//    activeBlocks(startingTips(genesis).last) = 0
//
//    val cbIndex = TrieMap[SignedObservationEdge, CheckpointBlock]()
//    //cbIndex(go.initialDistribution.soe) = go.initialDistribution
//    //cbIndex(go.initialDistribution2.soe) = go.initialDistribution2
//    var blockId = 3
//
//    val convMap = TrieMap[String, Int]()
//
//    val snapshotHeightInterval = 10
//
//    while (blockNum < maxNumBlocks) {
//
//      blockNum += 1
//      val tips = Random.shuffle(activeBlocks.toList).take(2)
//
//      tips.foreach {
//        case (tip, numUses) =>
//          def doRemove(): Unit = activeBlocks.remove(tip)
//
//          if (width < maxWidth) {
//            if (numUses >= 2) {
//              doRemove()
//            } else {
//              activeBlocks(tip) += 1
//            }
//          } else {
//            doRemove()
//          }
//
//      }
//
//      val block = randomBlock(tips.map {
//        _._1
//      }.toSeq)
//      cbIndex(block.soe) = block
//      activeBlocks(block.soe) = 0
//
//      convMap(block.soe.hash) = blockId
//      blockId += 1
//      width = activeBlocks.size
//
//      block
//
//    }
//
//    logger.debug(cbIndex.size.toString)
//
//    val genIdMap = Map(
//      genesis.genesis.soe.hash -> 0,
//      genesis.initialDistribution.soe.hash -> 1,
//      genesis.initialDistribution2.soe.hash -> 2
//    )
//
//    val conv = convMap.toMap ++ genIdMap /*cbIndex.toSeq.zipWithIndex.map{
//      case ((soe, cb), id) =>
//        soe.hash -> (id + 3)
//    }.toMap*/
//
//    val json = (cbIndex.toSeq.map {
//      case (soe, cb) =>
//        logger.debug(s"${soe.hash}")
//        logger.debug(s"${cb.parentSOEHashes}")
//        Map("id" -> conv(soe.hash), "parentIds" -> cb.parentSOEHashes.map {
//          conv
//        })
//    } ++ Seq(
//      Map("id" -> conv(genesis.initialDistribution.soe.hash), "parentIds" -> Seq(conv(genesis.genesis.soe.hash))),
//      Map("id" -> conv(genesis.initialDistribution2.soe.hash), "parentIds" -> Seq(conv(genesis.genesis.soe.hash))),
//      Map("id" -> conv(genesis.genesis.soe.hash), "parentIds" -> Seq[String]())
//    )).json
//    logger.debug(json)
//
//    //  file"../d3-dag/test/data/dag.json".write(json)
//
//    //createTransaction()
//
//  }
//
//}
