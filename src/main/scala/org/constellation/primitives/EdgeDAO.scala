package org.constellation.primitives

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import akka.util.Timeout
import cats.effect.IO.RaiseError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.constellation.consensus._
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema._
import org.constellation.storage._
import org.constellation.storage.transactions.TransactionGossiping
import org.constellation.util.Metrics
import org.constellation.{
  ConfigUtil,
  ConstellationConcurrentEffect,
  ConstellationContextShift,
  DAO,
  NodeConfig,
  ProcessingConfig
}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

class ThreadSafeMessageMemPool() extends StrictLogging {

  private var messages = Seq[Seq[ChannelMessage]]()

  val activeChannels: TrieMap[String, Semaphore] = TrieMap()

  val selfChannelNameToGenesisMessage: TrieMap[String, ChannelMessage] = TrieMap()
  val selfChannelIdToName: TrieMap[String, String] = TrieMap()

  val messageHashToSendRequest: TrieMap[String, ChannelSendRequest] = TrieMap()

  def release(messages: Seq[ChannelMessage]): Unit =
    messages.foreach { m =>
      activeChannels.get(m.signedMessageData.data.channelId).foreach {
        _.release()
      }

    }

  // TODO: Fix
  def pull(minCount: Int = 1): Option[Seq[ChannelMessage]] = this.synchronized {
    /*if (messages.size >= minCount) {
      val (left, right) = messages.splitAt(minCount)
      messages = right
      Some(left.flatten)
    } else None*/
    val flat = messages.flatten
    messages = Seq()
    if (flat.isEmpty) None
    else {
      logger.info(s"Pulled messages from mempool: ${flat.map {
        _.signedMessageData.hash
      }}")
      Some(flat)
    }
  }

  def batchPutDebug(messagesToAdd: Seq[ChannelMessage]): Boolean = this.synchronized {
    //messages ++= messagesToAdd
    true
  }

  def put(message: Seq[ChannelMessage], overrideLimit: Boolean = false)(implicit dao: DAO): Boolean =
    this.synchronized {
      val notContained = !messages.contains(message)

      if (notContained) {
        if (overrideLimit) {
          // Prepend in front to process user TX first before random ones
          messages = Seq(message) ++ messages

        } else if (messages.size < dao.processingConfig.maxMemPoolSize) {
          messages :+= message
        }
      }
      notContained
    }

  def unsafeCount: Int = messages.size

}

import constellation._
// TODO: wkoszycki this one is temporary till (#412 Flatten checkpointBlock in CheckpointCache) is finished
case class PendingDownloadException(id: Id)
    extends Exception(s"Node [${id.short}] is not ready to accept blocks from others.")
case object MissingCheckpointBlockException extends Exception("CheckpointBlock object is empty.")
case class MissingHeightException(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock ${cb.baseHash} height is missing for soeHash ${cb.soeHash}.")
case class PendingAcceptance(cbBaseHash: String)
    extends Exception(s"CheckpointBlock: $cbBaseHash is already pending acceptance phase.")
case class CheckpointAcceptBlockAlreadyStored(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock: ${cb.baseHash} is already stored.")

trait EdgeDAO {

  var metrics: Metrics

  @volatile var nodeConfig: NodeConfig

  def processingConfig: ProcessingConfig = nodeConfig.processingConfig

  private val blockFormationLock: Any = new Object()

  private[this] var _blockFormationInProgress: Boolean = false

  def blockFormationInProgress: Boolean = blockFormationLock.synchronized {
    _blockFormationInProgress
  }

  def blockFormationInProgress_=(value: Boolean): Unit = blockFormationLock.synchronized {
    _blockFormationInProgress = value
    metrics.updateMetric("blockFormationInProgress", blockFormationInProgress.toString)
  }

  implicit val contextShift: ContextShift[IO] = ConstellationContextShift.edge

  // TODO: Put on Id keyed datastore (address? potentially) with other metadata
  val publicReputation: TrieMap[Id, Double] = TrieMap()
  val secretReputation: TrieMap[Id, Double] = TrieMap()

  val otherNodeScores: TrieMap[Id, TrieMap[Id, Double]] = TrieMap()

  var cluster: Cluster[IO] = _
  var transactionService: TransactionService[IO] = _
  var transactionGossiping: TransactionGossiping[IO] = _
  var transactionGenerator: TransactionGenerator[IO] = _
  var checkpointService: CheckpointService[IO] = _
  var snapshotService: SnapshotService[IO] = _
  var rateLimiting: RateLimiting[IO] = _
  var addressService: AddressService[IO] = _

  val notificationService = new NotificationService[IO]()
  val messageService: MessageService[IO]
  val channelService = new ChannelService[IO]()
  val soeService = new SOEService[IO]()

  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)

  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] =
    threadSafeMessageMemPool.pull(minimumCount)

}
