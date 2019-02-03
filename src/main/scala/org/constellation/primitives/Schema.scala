package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.time.Instant

import constellation.pubKeyToAddress
import org.constellation.DAO
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils.hexToPublicKey
import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.util._

import scala.collection.concurrent.TrieMap
import scala.util.Random

// This can't be a trait due to serialization issues.

/** Documentation. */
object Schema {

  /** Documentation. */
  case class TreeVisual(
                         name: String,
                         parent: String,
                         children: Seq[TreeVisual]
                       )

  /** Documentation. */
  case class TransactionQueryResponse(
                                       hash: String,
                                       transaction: Option[Transaction],
                                       inMemPool: Boolean,
                                       inDAG: Boolean,
                                       cbEdgeHash: Option[String]
                                     )

  /** Documentation. */
  object NodeState extends Enumeration {
    type NodeState = Value
    val PendingDownload, DownloadInProgress, DownloadCompleteAwaitingFinalSync, Ready = Value
  }

  /** Documentation. */
  sealed trait ValidationStatus

  /** Documentation. */
  final case object Valid extends ValidationStatus

  /** Documentation. */
  final case object MempoolValid extends ValidationStatus

  /** Documentation. */
  final case object Unknown extends ValidationStatus

  /** Documentation. */
  final case object DoubleSpend extends ValidationStatus

  /** Documentation. */
  sealed trait ConfigUpdate

  /** Documentation. */
  final case class ReputationUpdates(updates: Seq[UpdateReputation]) extends ConfigUpdate

  /** Documentation. */
  case class UpdateReputation(id: Id, secretReputation: Option[Double], publicReputation: Option[Double])

  // I.e. equivalent to number of sat per btc
  val NormalizationFactor: Long = 1e8.toLong

  /** Documentation. */
  case class SendToAddress(
                            dst: String,
                            amount: Long,
                            normalized: Boolean = true
                          ) {

    /** Documentation. */
    def amountActual: Long = if (normalized) amount * NormalizationFactor else amount
  }

  // TODO: We also need a hash pointer to represent the post-tx counter party signing data, add later
  // TX should still be accepted even if metadata is incorrect, it just serves to help validation rounds.

  /** Documentation. */
  case class AddressMetaData(
                              address: String,
                              balance: Long = 0L,
                              lastValidTransactionHash: Option[String] = None,
                              txHashPool: Seq[String] = Seq(),
                              txHashOverflowPointer: Option[String] = None,
                              oneTimeUse: Boolean = false,
                              depth: Int = 0
                            ) extends Signable {

    /** Documentation. */
    def normalizedBalance: Long = balance / NormalizationFactor
  }

  /** Our basic set of allowed edge hash types */
  object EdgeHashType extends Enumeration {
    type EdgeHashType = Value
    val AddressHash,
    CheckpointDataHash, CheckpointHash,
    TransactionDataHash, TransactionHash,
    ValidationHash, BundleDataHash, ChannelMessageDataHash = Value
  }

  /** Documentation. */
  case class BundleEdgeData(rank: Double, hashes: Seq[String])

  /**
    * Wrapper for encapsulating a typed hash reference
    * @param hash : String of hashed value
    * @param hashType : Strictly typed from set of allowed edge formats
    */

  /** Documentation. */
  case class TypedEdgeHash(hash: String, hashType: EdgeHashType)

  /**
    * Basic edge format for linking two hashes with an optional piece of data attached. Similar to GraphX format.
    * Left is topologically ordered before right
    * @param parents: HyperEdge parent references
    * @param data : Optional hash reference to attached information
    */

  /** Documentation. */
  case class ObservationEdge( // TODO: Consider renaming to ObservationHyperEdge or leave as is?
                              parents: Seq[TypedEdgeHash],
                              data: TypedEdgeHash
                            ) extends Signable

  /**
    * Encapsulation for all witness information about a given observation edge.
    * @param signatureBatch : Collection of validation signatures about the edge.
    */

  /** Documentation. */
  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends Signable {

    /** Documentation. */
    def withSignatureFrom(keyPair: KeyPair): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.withSignatureFrom(keyPair))

    /** Documentation. */
    def withSignature(hs: HashSignature): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.withSignature(hs))

    /** Documentation. */
    def plus(other: SignatureBatch): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other))

    /** Documentation. */
    def plus(other: SignedObservationEdge): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other.signatureBatch))

    /** Documentation. */
    def baseHash: String = signatureBatch.hash
  }

  /**
    * Holder for ledger update information about a transaction
    *
    * @param amount : Quantity to be transferred
    * @param salt : Ensure hash uniqueness
    */

  /** Documentation. */
  case class TransactionEdgeData(amount: Long, salt: Long = Random.nextLong()) extends Signable

  /**
    * Collection of references to transaction hashes
    * @param hashes : TX edge hashes
    */

  /** Documentation. */
  case class CheckpointEdgeData(hashes: Seq[String], messages: Seq[ChannelMessage] = Seq()) extends Signable

  /** Documentation. */
  case class CheckpointEdge(edge: Edge[CheckpointEdgeData]) {

    /** Documentation. */
    def plus(other: CheckpointEdge) = this.copy(edge = edge.plus(other.edge))
  }

  /** Documentation. */
  case class Address(address: String) extends Signable {

    /** Documentation. */
    override def hash: String = address
  }

  /** Documentation. */
  case class AddressCacheData(
                               balance: Long,
                               memPoolBalance: Long,
                               reputation: Option[Double] = None,
                               ancestorBalances: Map[String, Long] = Map(),
                               ancestorReputations: Map[String, Long] = Map(),
                               //    recentTransactions: Seq[String] = Seq(),
                               balanceByLatestSnapshot: Long = 0L
                             ) {

    /** Documentation. */
    def plus(previous: AddressCacheData): AddressCacheData = {
      this.copy(
        ancestorBalances =
          ancestorBalances ++ previous.ancestorBalances.filterKeys(k => !ancestorBalances.contains(k)),
        ancestorReputations =
          ancestorReputations ++ previous.ancestorReputations.filterKeys(k => !ancestorReputations.contains(k))
        //recentTransactions =
        //  recentTransactions ++ previous.recentTransactions.filter(k => !recentTransactions.contains(k))
      )
    }

  }
  // Instead of one balance we need a Map from soe hash to balance and reputation
  // These values should be removed automatically by eviction
  // We can maintain some kind of automatic LRU cache for keeping track of what we want to remove
  // override evict method, and clean up data.
  // We should also mark a given balance / rep as the 'primary' one.

  /** Documentation. */
  case class TransactionCacheData(
                                   transaction: Transaction,
                                   valid: Boolean = false,
                                   inMemPool: Boolean = false,
                                   inDAG: Boolean = false,
                                   inDAGByAncestor: Map[String, Boolean] = Map(),
                                   resolved: Boolean = false,
                                   cbBaseHash: Option[String] = None,
                                   cbForkBaseHashes: Set[String] = Set(),
                                   signatureForks : Set[Transaction] = Set(),
                                   rxTime: Long = System.currentTimeMillis()
                                 ) {

    /** Documentation. */
    def plus(previous: TransactionCacheData): TransactionCacheData = {
      this.copy(
        inDAGByAncestor = inDAGByAncestor ++ previous.inDAGByAncestor.filterKeys(k => !inDAGByAncestor.contains(k)),
        cbForkBaseHashes = (cbForkBaseHashes ++ previous.cbForkBaseHashes) -- cbBaseHash.map{ s => Set(s)}.getOrElse(Set()),
        signatureForks = (signatureForks ++ previous.signatureForks) - transaction,
        rxTime = previous.rxTime
      )
    }
  }

  /** Documentation. */
  case class Height(min: Long, max: Long)

  /** Documentation. */
  case class CommonMetadata(
                             valid: Boolean = true,
                             inDAG: Boolean = false,
                             resolved: Boolean = true,
                             resolutionInProgress: Boolean = false,
                             inMemPool: Boolean = false,
                             lastResolveAttempt: Option[Long] = None,
                             rxTime: Long = System.currentTimeMillis(), // TODO: Unify common metadata like this
                           )

  // TODO: Separate cache with metadata vs what is stored in snapshot.

  /** Documentation. */
  case class CheckpointCacheData(
                                  checkpointBlock: Option[CheckpointBlock] = None,
                         //         metadata: CommonMetadata = CommonMetadata(),
                         //         children: Set[String] = Set(),
                                  height: Option[Height] = None
                                ) {
/*

    /** Documentation. */
    def plus(previous: CheckpointCacheData): CheckpointCacheData = {
      this.copy(
        lastResolveAttempt = lastResolveAttempt.map{t => Some(t)}.getOrElse(previous.lastResolveAttempt),
        rxTime = previous.rxTime
      )
    }
*/

  }

  /** Documentation. */
  case class SignedObservationEdgeCache(signedObservationEdge: SignedObservationEdge, resolved: Boolean = false)

  /** Documentation. */
  case class PeerIPData(canonicalHostName: String, port: Option[Int])

  /** Documentation. */
  case class ValidPeerIPData(canonicalHostName: String, port: Int)

  /** Documentation. */
  case class GenesisObservation(
                                 genesis: CheckpointBlock,
                                 initialDistribution: CheckpointBlock,
                                 initialDistribution2: CheckpointBlock
                               ) {

    /** Documentation. */
    def notGenesisTips(tips: Seq[CheckpointBlock]): Boolean = {
      !tips.contains(initialDistribution) && !tips.contains(initialDistribution2)
    }

  }

  /** Documentation. */
  @deprecated("Needs to be removed after peer manager changes", "january")
  case class InternalHeartbeat(round: Long = 0L)

  /** Documentation. */
  case class MetricsResult(metrics: Map[String, String])

  /** Documentation. */
  case class Id(hex: String) {

    /** Documentation. */
    def short: String = hex.toString.slice(0, 5)

    /** Documentation. */
    def medium: String = hex.toString.slice(0, 10)

    /** Documentation. */
    def address: String = KeyUtils.publicKeyToAddressString(toPublicKey)

    /** Documentation. */
    def toPublicKey: PublicKey = hexToPublicKey(hex)
  }

  /** Documentation. */
  case class Node(address: String, host: String, port: Int)

  /** Documentation. */
  case class TransactionSerialized(hash: String, sender: String, receiver: String, amount: Long, signers: Set[String], time: Long)

  /** Documentation. */
  object TransactionSerialized {

    /** Documentation. */
    def apply(tx: Transaction): TransactionSerialized =
      new TransactionSerialized(tx.hash, tx.src.address, tx.dst.address, tx.amount,
        tx.signatures.map(_.address).toSet, Instant.now.getEpochSecond)
  }

}
