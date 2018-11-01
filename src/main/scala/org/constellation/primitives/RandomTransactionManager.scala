package org.constellation.primitives

import constellation._
import org.constellation.DAO
import org.constellation.consensus.SnapshotTrigger
import org.constellation.primitives.Schema.{Id, NodeState, SendToAddress}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

object RandomTransactionManager {

  def trigger(round: Long)(implicit dao: DAO): Future[Try[Any]] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    futureTryWithTimeoutMetric({

      if (dao.metricsManager != null) {

        val memPoolCount = dao.threadSafeTXMemPool.unsafeCount
        dao.metricsManager ! UpdateMetric("transactionMemPoolSize", memPoolCount.toString)
        if (memPoolCount < dao.processingConfig.maxMemPoolSize && dao.generateRandomTX && dao.nodeState == NodeState.Ready) {

          val peerQuery = dao.peerInfo.toSeq //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
          val peerIds = peerQuery.filter { case (_, pd) =>
            pd.peerMetadata.timeAdded < (System.currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
          }

          dao.metricsManager ! UpdateMetric("numPeersOnDAO", peerQuery.size.toString)
          dao.metricsManager ! UpdateMetric("numPeersOnDAOThatAreReady", peerIds.size.toString)

          if (peerIds.nonEmpty) {

            val txs = Seq.fill(dao.processingConfig.randomTXPerRound)(0).par.map { _ =>

              // TODO: Make deterministic buckets for tx hashes later to process based on node ids.
              // this is super easy, just combine the hashes with ID hashes and take the max with BigInt

              def getRandomPeer: (Id, PeerData) = peerIds(Random.nextInt(peerIds.size))

              val sendRequest = SendToAddress(getRandomPeer._1.address.address, Random.nextInt(1000).toLong + 1L, normalized = false)
              val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair, normalized = false)
              dao.metricsManager ! IncrementMetric("signaturesPerformed")
              dao.metricsManager ! IncrementMetric("randomTransactionsGenerated")
              dao.metricsManager ! IncrementMetric("sentTransactions")

              tx
              /*            // TODO: Change to transport layer call
        dao.peerManager ! APIBroadcast(
          _.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx),
          peerSubset = Set(getRandomPeer._1)
        )*/
            }

            txs.foreach { t =>
              dao.threadSafeTXMemPool.put(t)
            }

          } else {

            dao.metricsManager ! IncrementMetric("triedToGenerateTransactionsButHaveNoPeers")
          }
        }

        if (memPoolCount > dao.processingConfig.minCheckpointFormationThreshold && dao.generateRandomTX) {
          futureTryWithTimeoutMetric(
            SnapshotTrigger.formCheckpoint(),
            "formCheckpointFromRandomTXManager",
            timeoutSeconds = 30
          )(dao.edgeExecutionContext, dao)
        }
      }


    }, "randomTransactionLoop")
  }
}