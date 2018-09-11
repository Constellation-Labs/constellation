package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import constellation._
import org.constellation.Data
import org.constellation.primitives.Schema.{Id, InternalHeartbeat, SendToAddress}

import scala.util.Random

class RandomTransactionManager(peerManager: ActorRef, metricsManager: ActorRef, dao: Data)(
                              implicit val timeout: Timeout
) extends Actor {

  val random = new Random()

  override def receive: Receive = {

    /**
      * This spawns random transactions for simulating load. For testing purposes only.
      */
    case InternalHeartbeat =>

      val peerIds = (peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
      def getRandomPeer: (Id, PeerData) = peerIds(random.nextInt(peerIds.size))
      val sendRequest = SendToAddress(getRandomPeer._1.address.address, random.nextInt(10000).toLong)
      val tx = createTransactionSafeBatchOE(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair)
      metricsManager ! IncrementMetric("signaturesPerformed")
      metricsManager ! IncrementMetric("randomTransactionsGenerated")
      metricsManager ! IncrementMetric("sentTransactions")

      // TODO: Change to transport layer call
      peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx))

  }
}
