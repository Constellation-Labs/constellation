package org.constellation.util

import java.security.KeyPair

import constellation.SHA256Ext
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.constellation.Fixtures.{dummyTx, _}
import Partitioner._
import org.constellation.DAO
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.{Schema, Transaction}
import org.constellation.primitives.Schema.SendToAddress

import scala.util.Random

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
