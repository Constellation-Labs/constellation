package org.constellation.consensus

import java.security.KeyPair

import constellation._
import org.constellation.primitives.Schema.{CheckpointBlock, SignedObservationEdge}
import org.constellation.primitives.{Genesis, Schema}
import org.scalatest.FlatSpec

import scala.collection.concurrent.TrieMap
import scala.util.Random

import org.constellation.crypto.KeyUtils._

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

  def randomBlock(tips: Seq[SignedObservationEdge], startingKeyPair: KeyPair = keyPairs.head): Schema.CheckpointBlock = {
    val txs = Seq.fill(5)(randomTransaction)
    EdgeProcessor.createCheckpointBlock(txs, tips)(startingKeyPair)
  }

  "Signatures combiners" should "be unique" in {

    val cb = randomBlock(startingTips, keyPairs.head)
    val cb2SameSignature = randomBlock(startingTips, keyPairs.head)

   // val bogus = cb.plus(keyPairs.head).signatures
  //  bogus.foreach{println}

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
