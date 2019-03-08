package org.constellation.primitives

import java.util.concurrent.Semaphore

import akka.actor.ActorRef
import constellation._
import org.constellation.DAO
import org.constellation.consensus.CrossTalkConsensus.StartNewBlockCreationRound
import org.constellation.primitives.Schema.{InternalHeartbeat, NodeState, SendToAddress, _}
import org.constellation.util.Periodic

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class RandomTransactionManager(nodeActor: ActorRef,periodSeconds: Int = 1)(implicit dao: DAO)
    extends Periodic("RandomTransactionManager", periodSeconds) {

  def trigger(): Future[Any] = {
    Option(dao.peerManager).foreach {
      _ ! InternalHeartbeat(round)
    }
    Thread.currentThread().setName("RandomTransactionManager")

    generateLoop()

  }

  private var testChannels = Seq[String]()

  def generateRandomMessages(): Unit =
    if (round % dao.processingConfig.roundsPerMessage == 0) {
      val cm =
        if ((dao.threadSafeMessageMemPool.activeChannels.size + dao.threadSafeMessageMemPool.unsafeCount) < 5) {
          val newChannelName = "channel_ " + dao.threadSafeMessageMemPool.activeChannels.size
          val channelOpen = ChannelOpen(newChannelName)
          val genesis =
            ChannelMessage.create(channelOpen.json, Genesis.CoinBaseHash, newChannelName)
          val genesisHash = genesis.signedMessageData.hash
          testChannels :+= genesisHash
          dao.threadSafeMessageMemPool.selfChannelIdToName(genesisHash) =
            newChannelName
          dao.threadSafeMessageMemPool.selfChannelNameToGenesisMessage(newChannelName) = genesis
          dao.threadSafeMessageMemPool.activeChannels(genesisHash) =
            new Semaphore(1)
          Some(genesis)
        } else {
          if (dao.threadSafeMessageMemPool.unsafeCount < 3) {
            val channels = dao.threadSafeMessageMemPool.activeChannels.filterKeys{testChannels.contains}
            if (channels.nonEmpty) {
              val (channel, lock) = channels.toList(Random.nextInt(channels.size))
              dao.messageService.get(channel).flatMap { data =>
                if (lock.tryAcquire()) {
                  Some(
                    ChannelMessage.create(Random.nextInt(1000).toString,
                      data.channelMessage.signedMessageData.signatures.hash,
                      channel)
                  )
                } else None
              }
            } else None
          } else None
        }
      cm.foreach { c =>
        dao.threadSafeMessageMemPool.put(Seq(c))
        dao.metrics.updateMetric("messageMemPoolSize",
                                 dao.threadSafeMessageMemPool.unsafeCount.toString)
      }
    }

  def generateLoop(): Future[Try[Unit]] = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    futureTryWithTimeoutMetric(
      {

        // Move elsewhere
        val peerIds = dao.readyPeers.toSeq.filter {
          case (_, pd) =>
            pd.peerMetadata.timeAdded < (System
              .currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000)
        }
        dao.metrics.updateMetric("numPeersOnDAO", dao.peerInfo.size.toString)
        dao.metrics.updateMetric("numPeersOnDAOThatAreReady", peerIds.size.toString)

        if ((peerIds.nonEmpty || dao.nodeConfig.isGenesisNode) && dao.nodeState == NodeState.Ready && dao.generateRandomTX) {

          generateRandomMessages()

          val memPoolCount = dao.threadSafeTXMemPool.unsafeCount
          dao.metrics.updateMetric("transactionMemPoolSize", memPoolCount.toString)

          val haveBalance =
            dao.addressService.get(dao.selfAddressStr).exists(_.balanceByLatestSnapshot > 10000000)

          if (memPoolCount < dao.processingConfig.maxMemPoolSize && haveBalance) {

            val numTX = (dao.processingConfig.randomTXPerRoundPerPeer / (peerIds.size + 1)) + 1
            Seq.fill(numTX)(0).foreach {
              _ =>
                // TODO: Make deterministic buckets for tx hashes later to process based on node ids.
                // this is super easy, just combine the hashes with ID hashes and take the max with BigInt

                def getRandomPeerAddress: String = {
                  if (dao.nodeConfig.isGenesisNode && peerIds.isEmpty) {
                    dao.dummyAddress
                  } else {
                    peerIds(Random.nextInt(peerIds.size))._1.address
                  }
                }

                val sendRequest = SendToAddress(getRandomPeerAddress,
                                                Random.nextInt(1000).toLong + 1L,
                                                normalized = false)
                val tx = createTransaction(dao.selfAddressStr,
                                           sendRequest.dst,
                                           sendRequest.amount,
                                           dao.keyPair,
                                           normalized = false)
                dao.metrics.incrementMetric("signaturesPerformed")
                dao.metrics.incrementMetric("randomTransactionsGenerated")
                dao.metrics.incrementMetric("sentTransactions")

                dao.threadSafeTXMemPool.put(tx)

                dao.transactionService.put(
                  tx.hash,
                  TransactionCacheData(
                    tx,
                    valid = true,
                    inMemPool = true
                  )
                )
                dao.peerInfo.foreach {
                  case (_, peerData) =>
                    dao.metrics.incrementMetric("transactionPut")
                    peerData.client.put("transaction", tx)
                }

                /*            // TODO: Change to transport layer call
    dao.peerManager ! APIBroadcast(
      _.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx),
      peerSubset = Set(getRandomPeer._1)
    )*/
            }
          }

          if (memPoolCount > dao.processingConfig.minCheckpointFormationThreshold &&
            dao.generateRandomTX &&
            dao.nodeState == NodeState.Ready &&
            !dao.blockFormationInProgress) {

            nodeActor ! StartNewBlockCreationRound
            dao.metrics.updateMetric("blockFormationInProgress",
              dao.blockFormationInProgress.toString)

          }
        }
      },
      "randomTransactionLoop"
    )
  }
}
