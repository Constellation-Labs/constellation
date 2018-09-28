package org.constellation.consensus

import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Schema.{CheckpointBlock, SignedObservationEdge}
import org.constellation.primitives.{Genesis, Schema}

import scala.collection.concurrent.TrieMap
import scala.util.Random


class RandomDataTest extends FlatSpec {


  private val keyPairs = Seq.fill(10)(makeKeyPair())

  private val go = Genesis.createGenesisAndInitialDistributionDirect(
    keyPairs.head.address.address,
    keyPairs.tail.map{_.getPublic.toId}.toSet,
    keyPairs.head
  )

  private val startingTips = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  def randomTransaction: Schema.Transaction = {
    val src = Random.shuffle(keyPairs).head
    createTransaction(
      src.address.address,
      Random.shuffle(keyPairs).head.address.address,
      Random.nextInt(500).toLong,
      src
    )
  }

  "Generate random CBs" should "build a graph" in {


    var width = 2
    val maxWidth = 6

    def randomBlock(tips: Seq[SignedObservationEdge]): Schema.CheckpointBlock = {
      val txs = Seq.fill(5)(randomTransaction)
      EdgeProcessor.createCheckpointBlock(txs, tips)(keyPairs.head)
    }

    val activeBlocks = TrieMap[SignedObservationEdge, Int]()


    val maxNumBlocks = 40
    var blockNum = 0

    activeBlocks(startingTips.head) = 0
    activeBlocks(startingTips.last) = 0

    val cbIndex = TrieMap[SignedObservationEdge, CheckpointBlock]()
    //cbIndex(go.initialDistribution.soe) = go.initialDistribution
    //cbIndex(go.initialDistribution2.soe) = go.initialDistribution2
    var blockId = 3

    val convMap = TrieMap[String, Int]()

    while (blockNum < maxNumBlocks) {

      blockNum += 1
      val tips = Random.shuffle(activeBlocks).take(2)

      tips.foreach{ case (tip, numUses) =>

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

      val block = randomBlock(tips.map{_._1}.toSeq)
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

    val conv = convMap.toMap ++ genIdMap  /*cbIndex.toSeq.zipWithIndex.map{
      case ((soe, cb), id) =>
        soe.hash -> (id + 3)
    }.toMap*/

    println((cbIndex.toSeq.map{
      case (soe, cb) =>
        Map("id" -> conv(soe.hash), "parentIds" -> cb.parentSOEHashes.map{conv})
    } ++ Seq(
      Map("id" -> conv(go.initialDistribution.soe.hash), "parentIds" -> Seq(conv(go.genesis.soe.hash))),
      Map("id" -> conv(go.initialDistribution2.soe.hash), "parentIds" -> Seq(conv(go.genesis.soe.hash))),
      Map("id" -> conv(go.genesis.soe.hash), "parentIds" -> Seq[String]())
    )).json)


    //createTransaction()


  }


}
