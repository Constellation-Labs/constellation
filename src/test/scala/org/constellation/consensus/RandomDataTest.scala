package org.constellation.consensus

import org.scalatest.FlatSpec
import constellation._
import org.constellation.primitives.Schema.SignedObservationEdge
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
    val maxWidth = 30

    def randomBlock(tips: Seq[SignedObservationEdge]): Schema.CheckpointBlock = {
      val txs = Seq.fill(5)(randomTransaction)
      EdgeProcessor.createCheckpointBlock(txs, tips)(keyPairs.head)
    }

    val activeBlocks = TrieMap[SignedObservationEdge, Int]()


    val maxNumBlocks = 1000
    var blockNum = 0

    activeBlocks(startingTips.head) = 0
    activeBlocks(startingTips.last) = 0

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
      activeBlocks(block.soe) = 0

    }




    //createTransaction()


  }


}
