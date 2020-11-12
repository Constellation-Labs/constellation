package org.constellation.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.constellation.Fixtures._
import Partitioner._
import org.constellation.schema.v2.transaction.Transaction
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PartitionerTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val random = new java.util.Random()

  def getRandomTxs(factor: Int = 5): Set[Transaction] = idSet5.flatMap { id =>
    val destinationAddresses = idSet5.map(_.address)
    val destinationAddressDups = (0 to factor).flatMap(_ => destinationAddresses)
    destinationAddressDups.map(
      destStr => makeTransaction(id.address, destStr, random.nextLong(), getRandomElement(tempKeySet, random))
    )
  }
  val randomTxs = getRandomTxs()
  val acceptableFacilBalance = 0.8
  val ids = idSet5.toList

  "Facilitator selection" should "be deterministic" in {
    val facilitator = selectTxFacilitator(ids, randomTxs.head)
    val facilitatorDup = selectTxFacilitator(ids, randomTxs.head)
    assert(facilitator === facilitatorDup)
  }

  "Facilitators" should "not facilitate their own transactions" in {
    val facilitator = selectTxFacilitator(ids, randomTxs.head)
    assert(facilitator.address != randomTxs.head.src.address)
  }

  "Facilitator selection" should "be relatively balanced" in {
    val facilitators = randomTxs.map(tx => selectTxFacilitator(ids, tx))
    assert(facilitators.size == 5)
  }

  "The gossip path" should "always be shorter then the total set of node ids" in {
    val pathLengths = randomTxs.map(gossipPath(ids, _).size)
    pathLengths.foreach(println)
    assert(pathLengths.forall(_ < ids.size))
  }
}
