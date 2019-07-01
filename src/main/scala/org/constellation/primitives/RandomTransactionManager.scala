package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.Semaphore

import akka.actor.ActorRef
import com.softwaremill.sttp.Response
import constellation._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.consensus.CrossTalkConsensus.StartNewBlockCreationRound
import org.constellation.primitives.Schema.{InternalHeartbeat, NodeState, _}
import org.constellation.storage.transactions.TransactionStatus
import org.constellation.util.{Distance, Periodic}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class RandomTransactionManager[T](nodeActor: ActorRef, periodSeconds: Int = 10)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("RandomTransactionManager", periodSeconds) {

  def trigger(): Future[Try[Unit]] = {
    Option(dao.peerManager).foreach {
      _ ! InternalHeartbeat(round)
    }
    Thread.currentThread().setName("RandomTransactionManager")

    generateLoop()

  }

  // TODO: Config
  val multiAddressGenerationMode = false

  private var testChannels = Seq[String]()

  def generateRandomMessages(): Unit =
    if (round % dao.processingConfig.roundsPerMessage == 0) {
      val cm =
        if ((dao.threadSafeMessageMemPool.activeChannels.size + dao.threadSafeMessageMemPool.unsafeCount) < 5) {
          val newChannelName = "channel_ " + dao.threadSafeMessageMemPool.activeChannels.size
          val channelOpen = ChannelOpen(newChannelName)
          val genesis =
            ChannelMessage.create(channelOpen.json, Genesis.CoinBaseHash, newChannelName)(dao.keyPair)
          val genesisHash = genesis.signedMessageData.hash
          testChannels :+= genesisHash
          dao.threadSafeMessageMemPool.selfChannelIdToName(genesisHash) = newChannelName
          dao.threadSafeMessageMemPool.selfChannelNameToGenesisMessage(newChannelName) = genesis
          dao.threadSafeMessageMemPool.activeChannels(genesisHash) = new Semaphore(1)
          Some(genesis)
        } else {
          if (dao.threadSafeMessageMemPool.unsafeCount < 3) {
            val channels = dao.threadSafeMessageMemPool.activeChannels.filterKeys {
              testChannels.contains
            }
            if (channels.nonEmpty) {
              val (channel, lock) = channels.toList(Random.nextInt(channels.size))
              dao.messageService.lookup(channel).unsafeRunSync().flatMap { data =>
                if (lock.tryAcquire()) {
                  Some(
                    ChannelMessage.create(
                      Random.nextInt(1000).toString,
                      data.channelMessage.signedMessageData.hash,
                      channel
                    )(dao.keyPair)
                  )
                } else None
              }
            } else None
          } else None
        }
      cm.foreach { c =>
        dao.threadSafeMessageMemPool.put(Seq(c))
        dao.metrics.updateMetric("messageMemPoolSize", dao.threadSafeMessageMemPool.unsafeCount.toString)
      }
    }

  def generateLoop(): Future[Try[Unit]] = {

    implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.edge

    futureTryWithTimeoutMetric(
      {

        // Move elsewhere
        val peerIds = dao.readyPeers.unsafeRunSync().toSeq.filter {
          case (_, pd) =>
            pd.peerMetadata.timeAdded < (System
              .currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000)
        }
        dao.metrics.updateMetric("numPeersOnDAO", dao.peerInfo.unsafeRunSync().size.toString)
        dao.metrics.updateMetric("numPeersOnDAOThatAreReady", peerIds.size.toString)

        if ((peerIds.nonEmpty || dao.nodeConfig.isGenesisNode) && dao.nodeState == NodeState.Ready && dao.generateRandomTX) {

          //generateRandomMessages()

          val pendingCount = dao.transactionService.count(TransactionStatus.Pending).unsafeRunSync()
          dao.metrics.updateMetric("transactionPendingSize", pendingCount.toString)

          val haveBalance =
            dao.addressService.lookup(dao.selfAddressStr).unsafeRunSync().exists(_.balanceByLatestSnapshot > 10000000)

          if (pendingCount < dao.processingConfig.maxMemPoolSize && haveBalance) {

            val numTX = (dao.processingConfig.randomTXPerRoundPerPeer / (peerIds.size + 1)) + 1
            Seq.fill(numTX)(0).foreach {
              _ =>
                // TODO: Make deterministic buckets for tx hashes later to process based on node ids.
                // this is super easy, just combine the hashes with ID hashes and take the max with BigInt

                def getRandomAddress: String =
                  if (dao.nodeConfig.isGenesisNode && peerIds.isEmpty) {
                    dao.dummyAddress
                  } else {

                    peerIds(Random.nextInt(peerIds.size))._1.address
                  }

                def simpleTX(src: String, kp: KeyPair = dao.keyPair) = createTransaction(
                  src,
                  getRandomAddress,
                  Random.nextInt(1000).toLong + 1L,
                  kp,
                  normalized = false
                )

                def txWithMultiAddress = {

                  val balancesForAddresses = dao.addresses.map { a =>
                    a -> dao.addressService.lookup(a).unsafeRunSync()
                  }
                  val auxAddressHaveSufficient = balancesForAddresses.forall { _._2.exists(_.balance > 10000000) }

                  if (!auxAddressHaveSufficient) {
                    val possibleDestinations = balancesForAddresses.filterNot { _._2.exists(_.balance > 10000000) }
                    val dst = Random.shuffle(possibleDestinations).head._1
                    createTransaction(
                      dao.selfAddressStr,
                      dst,
                      10,
                      dao.keyPair
                    )
                  } else {

                    val historyCheckPassable = balancesForAddresses.forall {
                      _._2.exists(_.balanceByLatestSnapshot > 10000000)
                    }

                    val randomSourceAddress = if (historyCheckPassable) {
                      dao.metrics.incrementMetric("historyCheckPassable")
                      Random.shuffle(dao.addresses :+ dao.selfAddressStr).head
                    } else dao.selfAddressStr

                    val srcKPMap = dao.addressToKeyPair + (dao.selfAddressStr -> dao.keyPair)

                    simpleTX(randomSourceAddress, srcKPMap(randomSourceAddress))

                  }
                }

                val tx = if (multiAddressGenerationMode) txWithMultiAddress else simpleTX(dao.selfAddressStr)

                // TODO: Unify this as an API call function equivalent
                /*                val sendRequest = SendToAddress(getRandomAddress,
                                                ,
                                                normalized = false)*/

                dao.metrics.incrementMetric("signaturesPerformed")
                dao.metrics.incrementMetric("randomTransactionsGenerated")
                dao.metrics.incrementMetric("sentTransactions")

                dao.transactionService
                  .put(TransactionCacheData(tx))
                  .unsafeRunSync()

                dao
                  .peerInfo(NodeType.Full)
                  .unsafeRunSync()
                  .values
                  .foreach { peerData ⇒
                    dao.metrics.incrementMetric("transactionPut")
                    peerData.client.put("transaction", tx)
                  }

                if (dao.peerInfo(NodeType.Light).unsafeRunSync().nonEmpty) {
                  val lightPeerData =
                    dao.peerInfo(NodeType.Light).unsafeRunSync().minBy(p ⇒ Distance.calculate(p._1, dao.id))._2
                  dao.metrics.incrementMetric("transactionPut")
                  dao.metrics.incrementMetric("transactionPutToLightNode")
                  lightPeerData.client.put("transaction", tx)
                }

                /*            // TODO: Change to transport layer call
    dao.peerManager ! APIBroadcast(
      _.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx),
      peerSubset = Set(getRandomPeer._1)
    )*/
            }
          }
        }
      },
      "randomTransactionLoop"
    )
  }
}
