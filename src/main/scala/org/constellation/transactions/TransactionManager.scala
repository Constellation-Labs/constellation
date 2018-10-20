package org.constellation.transactions

import constellation._
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor.HandleTransaction
import org.constellation.primitives.APIBroadcast
import org.constellation.primitives.Schema.SendToAddress

object TransactionManager {

  def handleSendToAddress(sendRequest: SendToAddress, dao: DAO): Unit = {
    val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair)

    dao.edgeProcessor ! HandleTransaction(tx)

    // TODO: Change to transport layer call
    dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx))
  }

}
