package org.constellation.primitives

import java.util.concurrent.Semaphore

import constellation._
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor
import org.constellation.primitives.Schema.{Id, NodeState, SendToAddress}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

/** Random transaction manager object. */
object RandomTransactionManager {

  /** Generator.
    *
    * @param round ... Integer to be factored by roundsPerMessage.
    * @param dao   ... Data access object.
    */
  def generateRandomMessages(round: Long)(implicit dao: DAO): Unit =
    if (round % dao.processingConfig.roundsPerMessage == 0) {
      val cm = if (
        (dao.threadSafeMessageMemPool.activeChannels.size + dao.threadSafeMessageMemPool.unsafeCount) < 3
      ) {
        val newChannelId = dao.selfAddressStr + dao.threadSafeMessageMemPool.activeChannels.size
        dao.threadSafeMessageMemPool.activeChannels(newChannelId) = new Semaphore(1)
        Some(ChannelMessage.create(Random.nextInt(1000).toString, Genesis.CoinBaseHash, newChannelId))
      } else {
        if (dao.threadSafeMessageMemPool.unsafeCount < 3) {
          val channels = dao.threadSafeMessageMemPool.activeChannels
          val (channel, lock) = Random.shuffle(channels).head
          dao.messageService.get(channel).flatMap { data =>
            if (lock.tryAcquire()) {
              Some(ChannelMessage.create(Random.nextInt(1000).toString, data.channelMessage.signedMessageData.signatures.hash, channel))
            } else None
          }
        } else None
      }
      cm.foreach { c =>
        dao.threadSafeMessageMemPool.put(c)
        dao.metricsManager ! UpdateMetric("messageMemPoolSize", dao.threadSafeMessageMemPool.unsafeCount.toString)
      }
    }

  /** Trigger.
    *
    * @param round ... Integer to be factored by roundsPerMessage in generateRandomMessages call.
    * @param dao   ... Data access object.
    */
  def trigger(round: Long)(implicit dao: DAO): Future[Try[Unit]] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    futureTryWithTimeoutMetric({

      if (dao.metricsManager != null) {
        // Move elsewhere
        val peerIds = dao.readyPeers.toSeq.filter { case (_, pd) =>
          pd.peerMetadata.timeAdded < (System.currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000)
        }
        dao.metricsManager ! UpdateMetric("numPeersOnDAO", dao.peerInfo.size.toString)
        dao.metricsManager ! UpdateMetric("numPeersOnDAOThatAreReady", peerIds.size.toString)

        if (peerIds.nonEmpty && dao.nodeState == NodeState.Ready && dao.generateRandomTX) {

          generateRandomMessages(round)

          val memPoolCount = dao.threadSafeTXMemPool.unsafeCount
          dao.metricsManager ! UpdateMetric("transactionMemPoolSize", memPoolCount.toString)
          if (memPoolCount < dao.processingConfig.maxMemPoolSize) {

            val numTX = (dao.processingConfig.randomTXPerRoundPerPeer / peerIds.size) + 1
            Seq.fill(numTX)(0).foreach { _ =>

              // TODO: Make deterministic buckets for tx hashes later to process based on node ids.
              // this is super easy, just combine the hashes with ID hashes and take the max with BigInt

              // doc
              def getRandomPeer: (Id, PeerData) = peerIds(Random.nextInt(peerIds.size))

              val sendRequest = SendToAddress(getRandomPeer._1.address.address, Random.nextInt(1000).toLong + 1L, normalized = false)
              val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair, normalized = false)
              dao.metricsManager ! IncrementMetric("signaturesPerformed")
              dao.metricsManager ! IncrementMetric("randomTransactionsGenerated")
              dao.metricsManager ! IncrementMetric("sentTransactions")

              dao.threadSafeTXMemPool.put(tx)

              /* // TODO: Change to transport layer call // tmp comment
                dao.peerManager ! APIBroadcast(
                  _.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx), peerSubset = Set(getRandomPeer._1)
                )
              */
            }
          }

          if (
            memPoolCount > dao.processingConfig.minCheckpointFormationThreshold &&
              dao.generateRandomTX &&
              dao.nodeState == NodeState.Ready &&
              !dao.blockFormationInProgress
          ) {

            dao.blockFormationInProgress = true

            val messages = dao.threadSafeMessageMemPool.pull(1).getOrElse(Seq())
            futureTryWithTimeoutMetric(
              EdgeProcessor.formCheckpoint(messages),
              "formCheckpointFromRandomTXManager",
              timeoutSeconds = dao.processingConfig.formCheckpointTimeout,
              {
                messages.foreach { m =>
                  dao.threadSafeMessageMemPool.activeChannels(m.signedMessageData.data.channelId).release()
                }
                dao.blockFormationInProgress = false
              }
            )(dao.edgeExecutionContext, dao)
          }
          dao.metricsManager ! UpdateMetric("blockFormationInProgress", dao.blockFormationInProgress.toString)

        } // end if

      } // end if

    }, "randomTransactionLoop") // end futureTryWithTimeoutMetric

  } // end trigger

} // end object RandomTransactionManager
