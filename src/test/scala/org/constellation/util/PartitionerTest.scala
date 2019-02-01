package org.constellation.util

import constellation.SHA256Ext
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.constellation.Fixtures._
import Partitioner._
import com.google.common.hash.Hashing
import org.constellation.DAO
import org.constellation.crypto.KeyUtils

class PartitionerTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit val dao: DAO = new DAO()

  dao.updateKeyPair(KeyUtils.makeKeyPair())
  dao.idDir.createDirectoryIfNotExists(createParents = true)
  dao.preventLocalhostAsPeer = false
  dao.externalHostString = ""
  dao.externlPeerHTTPPort = 0

  val tx = dummyTx(dao)
  val tx2 = dummyTx(dao, src = id2)
  val ids = idSet5.toSeq

  "Facilitator selection" should "be deterministic" in {
    val facilitator = selectTxFacilitator(ids, tx)
    val facilitatorDup = selectTxFacilitator(ids, tx)
    assert(facilitator === facilitatorDup)
  }

  "Facilitators" should "not facilitate their own transactions" in {
    val facilitator = selectTxFacilitator(ids, tx)
    assert(facilitator.address.address != tx.src.address)
  }

  "Facilitator selection" should "be relatively balanced" in {
    val txs = idSet5.map(srcId => dummyTx(dao, src = srcId))
    val facilitators = txs.map(tx => selectTxFacilitator(ids, tx))
    assert(facilitators.nonEmpty)//Todo, need to balance bucketing
  }


}
