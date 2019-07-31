package org.constellation.consensus

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.crypto.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.Metrics
import org.constellation.{DAO, NodeConfig}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.collection.concurrent.TrieMap
import scala.util.Random

object RandomData {

  val logger = Logger("RandomData")

  val keyPairs: Seq[KeyPair] = Seq.fill(10)(makeKeyPair())

  val go: GenesisObservation = Genesis.createGenesisAndInitialDistributionDirect(
    keyPairs.head.address,
    keyPairs.tail.map {
      _.getPublic.toId
    }.toSet,
    keyPairs.head
  )

  val startingTips: Seq[SignedObservationEdge] =
    Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  def randomBlock(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction)
    CheckpointBlock.createCheckpointBlock(txs, tips.map { s =>
      TypedEdgeHash(s.hash, EdgeHashType.CheckpointHash)
    })(startingKeyPair)
  }

  def randomTransaction: Transaction = {
    val src = Random.shuffle(keyPairs).head
    createTransaction(
      src.address,
      Random.shuffle(keyPairs).head.address,
      Random.nextInt(500).toLong,
      src
    )
  }

  def getAddress(keyPair: KeyPair): String = keyPair.address

  def fill(balances: Map[String, Long])(implicit dao: DAO): Iterable[Transaction] = {
    val txs = balances.map {
      case (address, amount) =>
        createTransaction(keyPairs.head.address, address, amount, keyPairs.head)
    }

    txs.toList
      .map(
        tx ⇒
          IO.pure(tx)
            .flatTap(dao.addressService.transfer)
            .flatTap(dao.addressService.transferSnapshot)
      )
      .sequence[IO, Transaction]
      .unsafeRunSync()

    txs
  }

  def setupSnapshot(cb: Seq[CheckpointBlock])(implicit dao: DAO): Seq[CheckpointBlock] = {
    dao.snapshotService.getSnapshotInfo
      .map(_.snapshot)
      .flatMap { snapshot =>
        dao.snapshotService.setSnapshot(
          SnapshotInfo(
            snapshot,
            cb.map(_.baseHash),
            Seq(),
            1,
            Seq(),
            Map.empty,
            Map.empty,
            Seq()
          )
        )
      }
      .unsafeRunSync()

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
      val tips = Random.shuffle(activeBlocks.toList).take(2)

      tips.foreach {
        case (tip, numUses) =>
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
