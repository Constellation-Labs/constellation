package org.constellation.transactions

import akka.actor.ActorRef
import org.constellation.Data
import org.constellation.primitives.{APIBroadcast, PeerManager}
import org.constellation.primitives.Schema.SendToAddress
import constellation._
import org.constellation.consensus.EdgeProcessor.HandleTransaction

import scala.concurrent.Future

object TransactionManager {

  def handleSendToAddress(sendRequest: SendToAddress, dao: Data): Unit = {
    val tx = createTransactionSafeBatch(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair)

    dao.edgeProcessor ! HandleTransaction(tx)

    // TODO: Change to transport layer call
    dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx))
  }

}
