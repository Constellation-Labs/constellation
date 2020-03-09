package org.constellation.primitives

import java.util.concurrent.Semaphore

import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.StrictLogging
import org.constellation.checkpoint.{
  CheckpointAcceptanceService,
  CheckpointBlockValidator,
  CheckpointParentService,
  CheckpointService
}
import org.constellation.consensus._
import org.constellation.datastore.SnapshotTrigger
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.redownload.{DownloadService, RedownloadService}
import org.constellation.domain.snapshot.{SnapshotInfoStorage, SnapshotStorage}
import org.constellation.p2p.{Cluster, JoiningPeerValidator}
import org.constellation.primitives.Schema._
import org.constellation.domain.transaction.{
  TransactionChainService,
  TransactionGossiping,
  TransactionService,
  TransactionValidator
}
import org.constellation.genesis.GenesisObservationWriter
import org.constellation.infrastructure.p2p.PeerHealthCheckWatcher
import org.constellation.infrastructure.redownload.RedownloadPeriodicCheck
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.rollback.{RollbackLoader, RollbackService}
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.rewards.EigenTrustStorage
import org.constellation.trust.{TrustDataPollingScheduler, TrustManager}
import org.constellation.util.{Metrics, SnapshotWatcher}
import org.constellation.{ConstellationExecutionContext, DAO, ProcessingConfig}

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

case class PendingDownloadException(id: Id)
    extends Exception(s"Node [${id.short}] is not ready to accept blocks from others.")

// TODO: wkoszycki this one is temporary till (#412 Flatten checkpointBlock in CheckpointCache) is finished
case object MissingCheckpointBlockException extends Exception("CheckpointBlock object is empty.")

case class MissingHeightException(baseHash: String, soeHash: String)
    extends Exception(s"CheckpointBlock ${baseHash} height is missing for soeHash ${soeHash}.")

object MissingHeightException {
  def apply(cb: CheckpointBlock): MissingHeightException = new MissingHeightException(cb.baseHash, cb.soeHash)
}

case class PendingAcceptance(baseHash: String)
    extends Exception(s"CheckpointBlock: $baseHash is already pending acceptance phase.")

object PendingAcceptance {
  def apply(cb: CheckpointBlock): PendingAcceptance = new PendingAcceptance(cb.baseHash)
}

case class CheckpointAcceptBlockAlreadyStored(baseHash: String)
    extends Exception(s"CheckpointBlock: ${baseHash} is already stored.")

object CheckpointAcceptBlockAlreadyStored {

  def apply(cb: CheckpointBlock): CheckpointAcceptBlockAlreadyStored =
    new CheckpointAcceptBlockAlreadyStored(cb.baseHash)
}

case class MissingTransactionReference(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock hash=${cb.baseHash} have missing transaction reference.")

object MissingTransactionReference {
  def apply(cb: CheckpointBlock): MissingTransactionReference = new MissingTransactionReference(cb)
}

case class MissingParents(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock hash=${cb.baseHash} have missing parents.")

object MissingParents {
  def apply(cb: CheckpointBlock): MissingParents = new MissingParents(cb)
}

case class ContainsInvalidTransactionsException(cbHash: String, txsToExclude: List[String])
    extends Exception(s"CheckpointBlock hash=$cbHash contains invalid transactions=$txsToExclude")

object ContainsInvalidTransactionsException {

  def apply(cb: CheckpointBlock, txsToExclude: List[String]): ContainsInvalidTransactionsException =
    new ContainsInvalidTransactionsException(cb.baseHash, txsToExclude)
}

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
  var trustManager: TrustManager[IO] = _
  var transactionService: TransactionService[IO] = _
  var blacklistedAddresses: BlacklistedAddresses[IO] = _
  var transactionChainService: TransactionChainService[IO] = _
  var transactionGossiping: TransactionGossiping[IO] = _
  var transactionGenerator: TransactionGenerator[IO] = _
  var observationService: ObservationService[IO] = _
  var checkpointService: CheckpointService[IO] = _
  var checkpointParentService: CheckpointParentService[IO] = _
  var checkpointAcceptanceService: CheckpointAcceptanceService[IO] = _
  var snapshotStorage: SnapshotStorage[IO] = _
  var snapshotInfoStorage: SnapshotInfoStorage[IO] = _
  var eigenTrustStorage: EigenTrustStorage[IO] = _
  var snapshotService: SnapshotService[IO] = _
  var concurrentTipService: ConcurrentTipService[IO] = _
  var checkpointBlockValidator: CheckpointBlockValidator[IO] = _
  var transactionValidator: TransactionValidator[IO] = _
  var rateLimiting: RateLimiting[IO] = _
  var addressService: AddressService[IO] = _
  var snapshotWatcher: SnapshotWatcher = _
  var rollbackService: RollbackService[IO] = _
  var cloudStorage: CloudStorage[IO] = _
  var redownloadService: RedownloadService[IO] = _
  var downloadService: DownloadService[IO] = _
  var peerHealthCheck: PeerHealthCheck[IO] = _
  var peerHealthCheckWatcher: PeerHealthCheckWatcher = _
  var genesisObservationWriter: GenesisObservationWriter[IO] = _
  var consensusRemoteSender: ConsensusRemoteSender[IO] = _
  var consensusManager: ConsensusManager[IO] = _
  var consensusWatcher: ConsensusWatcher = _
  var consensusScheduler: ConsensusScheduler = _
  var trustDataPollingScheduler: TrustDataPollingScheduler = _
  var eigenTrust: EigenTrust[IO] = _
  var rewardsManager: RewardsManager[IO] = _
  var joiningPeerValidator: JoiningPeerValidator[IO] = _
  var snapshotTrigger: SnapshotTrigger = _
  var redownloadPeriodicCheck: RedownloadPeriodicCheck = _
  var transactionGeneratorTrigger: TransactionGeneratorTrigger = _

  val notificationService = new NotificationService[IO]()
  val channelService = new ChannelService[IO]()
  val soeService = new SOEService[IO]()
  val recentBlockTracker = new RecentDataTracker[CheckpointCache](200)
  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  var rollbackLoader: RollbackLoader = _

  def maxWidth: Int = processingConfig.maxWidth

  def maxTXInBlock: Int = processingConfig.maxTXInBlock

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  def pullMessages(minimumCount: Int): Option[Seq[ChannelMessage]] =
    threadSafeMessageMemPool.pull(minimumCount)

}
