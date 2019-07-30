package org.constellation.primitives

import java.util.concurrent.Semaphore

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.InternalHeartbeat
import org.constellation.util.Periodic

import scala.concurrent.Future
import scala.util.{Random, Try}

class InternalHeartbeatTrigger[T](nodeActor: ActorRef, periodSeconds: Int = 10)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("InternalHeartbeatTrigger", periodSeconds)
    with StrictLogging {

  def trigger(): Future[Try[Unit]] = {
    Thread.currentThread().setName("InternalHeartbeatTrigger")
    dao.cluster.internalHearthbeat(round).map(_ => Try(())).unsafeToFuture
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
}
