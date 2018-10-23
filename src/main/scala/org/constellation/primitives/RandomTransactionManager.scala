package org.constellation.primitives

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{Id, InternalHeartbeat, NodeState, SendToAddress}
import org.constellation.util.HeartbeatSubscribe

import scala.util.Random

class RandomTransactionManager(dao: DAO)(
  implicit val timeout: Timeout
) extends Actor {

  dao.heartbeatActor ! HeartbeatSubscribe

  override def receive: Receive = {

    /**
      * This spawns random transactions for simulating load. For testing purposes only.
      */
    case InternalHeartbeat =>

      if (dao.transactionMemPool.size < 1000 && dao.generateRandomTX && dao.nodeState == NodeState.Ready) {
        val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq.filter { case (_, pd) =>
          pd.timeAdded < (System.currentTimeMillis() - 30 * 1000) && pd.nodeState == NodeState.Ready
        }

        if (peerIds.nonEmpty) {

          Seq.fill(dao.processingConfig.randomTXPerRound)(0).par.foreach { _ =>

            // TODO: Make deterministic buckets for tx hashes later to process based on node ids.
            // this is super easy, just combine the hashes with ID hashes and take the max with BigInt

            def getRandomPeer: (Id, PeerData) = peerIds(Random.nextInt(peerIds.size))

            val sendRequest = SendToAddress(getRandomPeer._1.address.address, Random.nextInt(1000).toLong + 1L, normalized = false)
            val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair, normalized = false)
            dao.metricsManager ! IncrementMetric("signaturesPerformed")
            dao.metricsManager ! IncrementMetric("randomTransactionsGenerated")
            dao.metricsManager ! IncrementMetric("sentTransactions")

            // TODO: Change to transport layer call
            dao.peerManager ! APIBroadcast(
              _.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx),
              peerSubset = Set(getRandomPeer._1)
            )
          }
        }
      }

  }
}
