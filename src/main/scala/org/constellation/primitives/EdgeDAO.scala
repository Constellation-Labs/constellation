package org.constellation.primitives

import java.util.concurrent.Semaphore

import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.StrictLogging
import org.constellation.consensus._
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema._
import org.constellation.rollback.RollbackService
import org.constellation.storage._
import org.constellation.storage.transactions.TransactionGossiping
import org.constellation.util.{Metrics, SnapshotWatcher}
import org.constellation.{ConstellationExecutionContext, DAO, NodeConfig, ProcessingConfig}

import scala.collection.concurrent.TrieMap

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
      logger.debug(s"Pulled messages from mempool: ${flat.map {
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

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  // TODO: Put on Id keyed datastore (address? potentially) with other metadata
  val publicReputation: TrieMap[Id, Double] = TrieMap()
  val secretReputation: TrieMap[Id, Double] = TrieMap()

  val otherNodeScores: TrieMap[Id, TrieMap[Id, Double]] = TrieMap()

  var ipManager: IPManager[IO] = _
  var cluster: Cluster[IO] = _
  var transactionService: TransactionService[IO] = _
  var transactionGossiping: TransactionGossiping[IO] = _
  var transactionGenerator: TransactionGenerator[IO] = _
  var observationService: ObservationService[IO] = _
  var checkpointService: CheckpointService[IO] = _
  var snapshotService: SnapshotService[IO] = _
  var concurrentTipService: ConcurrentTipService[IO] = _
  var rateLimiting: RateLimiting[IO] = _
  var addressService: AddressService[IO] = _
  var snapshotBroadcastService: SnapshotBroadcastService[IO] = _
  var snapshotWatcher: SnapshotWatcher = _
  var rollbackService: RollbackService[IO] = _

  var consensusRemoteSender: ConsensusRemoteSender[IO] = _
  var consensusManager: ConsensusManager[IO] = _
  var consensusWatcher: ConsensusWatcher = _
  var consensusScheduler: ConsensusScheduler = _
  val notificationService = new NotificationService[IO]()
  val messageService: MessageService[IO]
  val channelService = new ChannelService[IO]()
  val soeService = new SOEService[IO]()

  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)

  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] =
    threadSafeMessageMemPool.pull(minimumCount)

}
