package org.constellation.primitives

import java.security.KeyPair

import enumeratum._
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.schema.Id
import org.constellation.util._

// This can't be a trait due to serialization issues.
import scala.util.Random

object Schema {

  case class TreeVisual(
    name: String,
    parent: String,
    children: Seq[TreeVisual]
  )

  case class TransactionQueryResponse(
    hash: String,
    transaction: Option[Transaction],
    inMemPool: Boolean,
    inDAG: Boolean,
    cbEdgeHash: Option[String]
  )

  sealed trait NodeState extends EnumEntry

  object NodeState extends Enum[NodeState] with CirceEnum[NodeState] {

    case object PendingDownload extends NodeState
    case object ReadyForDownload extends NodeState
    case object DownloadInProgress extends NodeState
    case object DownloadCompleteAwaitingFinalSync extends NodeState
    case object SnapshotCreation extends NodeState
    case object Ready extends NodeState
    case object Leaving extends NodeState
    case object Offline extends NodeState

    val values = findValues

    val all: Set[NodeState] = values.toSet

    val readyStates: Set[NodeState] = Set(NodeState.Ready, NodeState.SnapshotCreation)

    val initial: Set[NodeState] = Set(Offline, PendingDownload)

    val broadcastStates: Set[NodeState] = Set(Ready, Leaving, Offline, PendingDownload, ReadyForDownload)

    val offlineStates: Set[NodeState] = Set(Offline)

    val invalidForJoining: Set[NodeState] = Set(Leaving, Offline)

    val validDuringDownload: Set[NodeState] =
      Set(ReadyForDownload, DownloadInProgress, DownloadCompleteAwaitingFinalSync)

    val validForDownload: Set[NodeState] = Set(PendingDownload, Ready)

    val validForRedownload: Set[NodeState] = Set(ReadyForDownload, Ready)

    val validForSnapshotCreation: Set[NodeState] = Set(Ready, Leaving)

    val validForTransactionGeneration: Set[NodeState] = Set(Ready, SnapshotCreation)

    val validForOwnConsensus: Set[NodeState] = Set(Ready, SnapshotCreation)

    val validForConsensusParticipation: Set[NodeState] = Set(Ready, SnapshotCreation)

    val validForLettingOthersDownload: Set[NodeState] = Set(Ready, SnapshotCreation, Leaving)

    val validForLettingOthersRedownload: Set[NodeState] = Set(Ready, Leaving)

    val validForCheckpointAcceptance: Set[NodeState] = Set(Ready, SnapshotCreation)

    val validForCheckpointPendingAcceptance: Set[NodeState] = validDuringDownload

    // TODO: Use initial for allowing joining after leaving
    def canJoin(current: NodeState): Boolean = current == PendingDownload

    def isNotOffline(current: NodeState): Boolean = !offlineStates.contains(current)

    def isInvalidForJoining(current: NodeState): Boolean = invalidForJoining.contains(current)

    def canActAsJoiningSource(current: NodeState): Boolean = all.diff(invalidForJoining).contains(current)

    def canActAsDownloadSource(current: NodeState): Boolean = validForLettingOthersDownload.contains(current)

    def canActAsRedownloadSource(current: NodeState): Boolean = validForLettingOthersRedownload.contains(current)

    def canRunClusterCheck(current: NodeState): Boolean = validForRedownload.contains(current)

    def canCreateSnapshot(current: NodeState): Boolean = validForSnapshotCreation.contains(current)

    def canGenerateTransactions(current: NodeState): Boolean = validForTransactionGeneration.contains(current)

    def canStartOwnConsensus(current: NodeState): Boolean = validForOwnConsensus.contains(current)

    def canParticipateConsensus(current: NodeState): Boolean = validForConsensusParticipation.contains(current)

    def canAcceptCheckpoint(current: NodeState): Boolean = validForCheckpointAcceptance.contains(current)

    def canAwaitForCheckpointAcceptance(current: NodeState): Boolean =
      validForCheckpointPendingAcceptance.contains(current)

  }

  sealed trait NodeType extends EnumEntry

  object NodeType extends Enum[NodeType] with CirceEnum[NodeType] {
    case object Full extends NodeType
    case object Light extends NodeType

    val values = findValues
  }

  sealed trait ValidationStatus

  final case object Valid extends ValidationStatus

  final case object MempoolValid extends ValidationStatus

  final case object Unknown extends ValidationStatus

  final case object DoubleSpend extends ValidationStatus

  sealed trait ConfigUpdate

  final case class ReputationUpdates(updates: Seq[UpdateReputation]) extends ConfigUpdate

  case class UpdateReputation(id: Id, secretReputation: Option[Double], publicReputation: Option[Double])

  // I.e. equivalent to number of sat per btc
  val NormalizationFactor: Long = 1e8.toLong

  case class SendToAddress(
    dst: String,
    amount: Long,
    normalized: Boolean = true
  ) {

    def amountActual: Long = if (normalized) amount * NormalizationFactor else amount
  }

  // TODO: We also need a hash pointer to represent the post-tx counter party signing data, add later
  // TX should still be accepted even if metadata is incorrect, it just serves to help validation rounds.

  case class AddressMetaData(
    address: String,
    balance: Long = 0L,
    lastValidTransactionHash: Option[String] = None,
    txHashPool: Seq[String] = Seq(),
    txHashOverflowPointer: Option[String] = None,
    oneTimeUse: Boolean = false,
    depth: Int = 0
  ) extends Signable {

    def normalizedBalance: Long = balance / NormalizationFactor
  }

  /** Our basic set of allowed edge hash types */
  sealed trait EdgeHashType extends EnumEntry

  object EdgeHashType extends Enum[EdgeHashType] with CirceEnum[EdgeHashType] {

    case object AddressHash extends EdgeHashType
    case object CheckpointDataHash extends EdgeHashType
    case object CheckpointHash extends EdgeHashType
    case object TransactionDataHash extends EdgeHashType
    case object TransactionHash extends EdgeHashType
    case object ValidationHash extends EdgeHashType
    case object BundleDataHash extends EdgeHashType
    case object ChannelMessageDataHash extends EdgeHashType

    val values = findValues
  }

  case class BundleEdgeData(rank: Double, hashes: Seq[String])

  /**
    * Wrapper for encapsulating a typed hash reference
    *
    * @param hashReference : String of hashed value or reference to be signed
    * @param hashType : Strictly typed from set of allowed edge formats
    */ // baseHash Temporary to debug heights missing
  case class TypedEdgeHash(
    hashReference: String,
    hashType: EdgeHashType,
    baseHash: Option[String] = None
  )

  /**
    * Basic edge format for linking two hashes with an optional piece of data attached. Similar to GraphX format.
    * Left is topologically ordered before right
    *
    * @param parents : HyperEdge parent references
    * @param data    : Optional hash reference to attached information
    */
  case class ObservationEdge( // TODO: Consider renaming to ObservationHyperEdge or leave as is?
    parents: Seq[TypedEdgeHash],
    data: TypedEdgeHash
  ) extends Signable {
    override def getEncoding = {
      val numParents = parents.length //note, we should not use magick number 2 here, state channels can have multiple
      val encodedParentHashRefs = runLengthEncoding(parents.map(_.hashReference): _*)
      numParents + encodedParentHashRefs + data.hashReference
    }
  }

  /**
    * Encapsulation for all witness information about a given observation edge.
    *
    * @param signatureBatch : Collection of validation signatures about the edge.
    */
  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends Signable {

    def withSignatureFrom(keyPair: KeyPair): SignedObservationEdge =
      this.copy(signatureBatch = signatureBatch.withSignatureFrom(keyPair))

    def withSignature(hs: HashSignature): SignedObservationEdge =
      this.copy(signatureBatch = signatureBatch.withSignature(hs))

    def plus(other: SignatureBatch): SignedObservationEdge =
      this.copy(signatureBatch = signatureBatch.plus(other))

    def plus(other: SignedObservationEdge): SignedObservationEdge =
      this.copy(signatureBatch = signatureBatch.plus(other.signatureBatch))

    def baseHash: String = signatureBatch.hash

    override def hash = signatureBatch.hash

  }

  /**
    * Holder for ledger update information about a transaction
    *
    * @param amount : Quantity to be transferred
    * @param salt : Ensure hash uniqueness
    */
  case class TransactionEdgeData(
    amount: Long,
    lastTxRef: LastTransactionRef,
    fee: Option[Long] = None,
    salt: Long = Random.nextLong()
  ) extends Signable {
    override def getEncoding = {
      val encodedAmount = runLengthEncoding(Seq(amount.toHexString): _*)
      val encodedFeeSalt = runLengthEncoding(Seq(fee.getOrElse(0L).toString, salt.toHexString): _*)
      encodedAmount + lastTxRef.getEncoding + encodedFeeSalt
    }
  }

  /**
    * Collection of references to transaction hashes
    *
    * @param hashes : TX edge hashes
    */
  case class CheckpointEdgeData(
    hashes: Seq[String],
    messageHashes: Seq[String] = Seq(),
    observationsHashes: Seq[String]
  ) extends Signable

  case class CheckpointEdge(edge: Edge[CheckpointEdgeData]) {

    def plus(other: CheckpointEdge) = this.copy(edge = edge.plus(other.edge))
  }

  case class Address(address: String) extends Signable {

    override def hash: String = address
  }

  case class AddressCacheData(
    balance: Long,
    memPoolBalance: Long,
    reputation: Option[Double] = None,
    ancestorBalances: Map[String, Long] = Map(),
    ancestorReputations: Map[String, Long] = Map(),
    //    recentTransactions: Seq[String] = Seq(),
    balanceByLatestSnapshot: Long = 0L,
    rewardsBalance: Long = 0L
  ) {

    def plus(previous: AddressCacheData): AddressCacheData =
      this.copy(
        ancestorBalances =
          ancestorBalances ++ previous.ancestorBalances
            .filterKeys(k => !ancestorBalances.contains(k)),
        ancestorReputations =
          ancestorReputations ++ previous.ancestorReputations.filterKeys(
            k => !ancestorReputations.contains(k)
          )
        //recentTransactions =
        //  recentTransactions ++ previous.recentTransactions.filter(k => !recentTransactions.contains(k))
      )

  }

  // Instead of one balance we need a Map from soe hash to balance and reputation
  // These values should be removed automatically by eviction
  // We can maintain some kind of automatic LRU cache for keeping track of what we want to remove
  // override evict method, and clean up data.
  // We should also mark a given balance / rep as the 'primary' one.

  case class Height(min: Long, max: Long) extends Ordered[Height] {
    override def compare(that: Height): Int = min.compare(that.min)
  }

  case class CommonMetadata(
    valid: Boolean = true,
    inDAG: Boolean = false,
    resolved: Boolean = true,
    resolutionInProgress: Boolean = false,
    inMemPool: Boolean = false,
    lastResolveAttempt: Option[Long] = None,
    rxTime: Long = System.currentTimeMillis() // TODO: Unify common metadata like this
  )

  // TODO: Separate cache with metadata vs what is stored in snapshot.

  case class CheckpointCacheMetadata(
    checkpointBlock: CheckpointBlockMetadata,
    children: Int = 0,
    height: Option[Height] = None
  )

  object CheckpointCache {
    implicit val checkpointCacheOrdering: Ordering[CheckpointCache] =
      Ordering.by[CheckpointCache, Long](_.height.fold(0L)(_.min))
  }
  case class CheckpointCache(
    checkpointBlock: CheckpointBlock,
    children: Int = 0,
    height: Option[Height] = None // TODO: Check if Option if needed
  ) {
    /*

    def plus(previous: CheckpointCacheData): CheckpointCacheData = {
      this.copy(
        lastResolveAttempt = lastResolveAttempt.map{t => Some(t)}.getOrElse(previous.lastResolveAttempt),
        rxTime = previous.rxTime
      )
    }
   */

  }

  case class PeerIPData(canonicalHostName: String, port: Option[Int])

  case class ValidPeerIPData(canonicalHostName: String, port: Int)

  case class GenesisObservation(
    genesis: CheckpointBlock,
    initialDistribution: CheckpointBlock,
    initialDistribution2: CheckpointBlock
  ) {

    def notGenesisTips(tips: Seq[CheckpointBlock]): Boolean =
      !tips.contains(initialDistribution) && !tips.contains(initialDistribution2)

  }

  @deprecated("Needs to be removed after peer manager changes", "january")
  case class InternalHeartbeat(round: Long = 0L)

  case class MetricsResult(metrics: Map[String, String])

  case class Node(address: String, host: String, port: Int)

}
