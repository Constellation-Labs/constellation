package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.time.Instant

import cats.data._
import cats.implicits._
import constellation.pubKeyToAddress
import org.constellation.DAO
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils.hexToPublicKey
import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.util._

import scala.collection.concurrent.TrieMap
import scala.util.Random

// This can't be a trait due to serialization issues
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



  object NodeState extends Enumeration {
    type NodeState = Value
    val PendingDownload, DownloadInProgress, DownloadCompleteAwaitingFinalSync, Ready = Value
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

  sealed trait Fiber

  case class TransactionData(
                              src: String,
                              dst: String,
                              amount: Long,
                              salt: Long = Random.nextLong() // To ensure hash uniqueness.
                            ) extends Signable with GossipMessage {
    def hashType = "TransactionData"
    def inverseAmount: Long = -1*amount
    def normalizedAmount: Long = amount / NormalizationFactor
    def pretty: String = s"TX: $short FROM: ${src.slice(0, 8)} " +
      s"TO: ${dst.slice(0, 8)} " +
      s"AMOUNT: $normalizedAmount"

    def srcLedgerBalance(ledger: TrieMap[String, Long]): Long = {
      ledger.getOrElse(src, 0)
    }

    def ledgerValid(ledger: TrieMap[String, Long]): Boolean = {
      srcLedgerBalance(ledger) >= amount
    }

    def updateLedger(ledger: TrieMap[String, Long]): Unit = {
      if (ledger.contains(src)) ledger(src) = ledger(src) - amount

      if (ledger.contains(dst)) ledger(dst) = ledger(dst) + amount
      else ledger(dst) = amount
    }

  }

  sealed trait GossipMessage

  // Participants are notarized via signatures.
  final case class BundleBlock(
                                parentHash: String,
                                height: Long,
                                txHash: Seq[String]
                              ) extends Signable with Fiber

  final case class BundleHash(hash: String) extends Fiber
  final case class TransactionHash(txHash: String) extends Fiber
  final case class ParentBundleHash(pbHash: String) extends Fiber

  // TODO: Make another bundle data with additional metadata for depth etc.
  final case class BundleData(bundles: Seq[Fiber]) extends Signable

  case class RequestBundleData(hash: String) extends GossipMessage
  case class HashRequest(hash: String) extends GossipMessage
  case class BatchHashRequest(hashes: Set[String]) extends GossipMessage

  case class BatchBundleHashRequest(hashes: Set[String]) extends GossipMessage
  case class BatchTXHashRequest(hashes: Set[String]) extends GossipMessage

  case class UnknownParentHashSyncInfo(
                                        firstRequestTime: Long,
                                        lastRequestTime: Long,
                                        numRequests: Int
                                      )





  /**
    * Our basic set of allowed edge hash types
    */
  object EdgeHashType extends Enumeration {
    type EdgeHashType = Value
    val AddressHash,
    CheckpointDataHash, CheckpointHash,
    TransactionDataHash, TransactionHash,
    ValidationHash, BundleDataHash, ChannelMessageDataHash = Value
  }

  case class BundleEdgeData(rank: Double, hashes: Seq[String])

  /**
    * Wrapper for encapsulating a typed hash reference
    * @param hash : String of hashed value
    * @param hashType : Strictly typed from set of allowed edge formats
    */
  case class TypedEdgeHash(hash: String, hashType: EdgeHashType)

  /**
    * Basic edge format for linking two hashes with an optional piece of data attached. Similar to GraphX format.
    * Left is topologically ordered before right
    * @param parents: HyperEdge parent references
    * @param data : Optional hash reference to attached information
    */
  case class ObservationEdge( // TODO: Consider renaming to ObservationHyperEdge or leave as is?
                              parents: Seq[TypedEdgeHash],
                              data: TypedEdgeHash
                            ) extends Signable

  /**
    * Encapsulation for all witness information about a given observation edge.
    * @param signatureBatch : Collection of validation signatures about the edge.
    */
  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends Signable {
    def plus(keyPair: KeyPair): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(keyPair))
    def plus(hs: HashSignature): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(hs))
    def plus(other: SignatureBatch): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other))
    def plus(other: SignedObservationEdge): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other.signatureBatch))
    def baseHash: String = signatureBatch.hash
  }

  /**
    * Holder for ledger update information about a transaction
    *
    * @param amount : Quantity to be transferred
    * @param salt : Ensure hash uniqueness
    */
  case class TransactionEdgeData(amount: Long, salt: Long = Random.nextLong()) extends Signable

  /**
    * Collection of references to transaction hashes
    * @param hashes : TX edge hashes
    */
  case class CheckpointEdgeData(hashes: Seq[String], messages: Seq[ChannelMessage] = Seq()) extends Signable

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
                               balanceByLatestSnapshot: Long = 0L
                             ) {

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
    def plus(previous: TransactionCacheData): TransactionCacheData = {
      this.copy(
        inDAGByAncestor = inDAGByAncestor ++ previous.inDAGByAncestor.filterKeys(k => !inDAGByAncestor.contains(k)),
        cbForkBaseHashes = (cbForkBaseHashes ++ previous.cbForkBaseHashes) -- cbBaseHash.map{ s => Set(s)}.getOrElse(Set()),
        signatureForks = (signatureForks ++ previous.signatureForks) - transaction,
        rxTime = previous.rxTime
      )
    }
  }

  case class Height(min: Long, max: Long)

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
  case class CheckpointCacheData(
                                  checkpointBlock: Option[CheckpointBlock] = None,
                         //         metadata: CommonMetadata = CommonMetadata(),
                         //         children: Set[String] = Set(),
                                  height: Option[Height] = None
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

  case class SignedObservationEdgeCache(signedObservationEdge: SignedObservationEdge, resolved: Boolean = false)

  sealed trait CheckpointBlockValidation {
    def errorMessage: String
  }

  case class EmptySignatures() extends CheckpointBlockValidation {
    def errorMessage: String = "CheckpointBlock has no signatures"
  }


  case class InvalidSignature(signature: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock includes signature=$signature which is invalid"
  }

  object InvalidSignature {
    def apply(s: HashSignature) = new InvalidSignature(s.signature)
  }


  case class InvalidTransaction(txHash: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which is invalid"
  }

  object InvalidTransaction {
    def apply(t: Transaction) = new InvalidTransaction(t.hash)
  }


  case class DuplicatedTransaction(txHash: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock includes duplicated transaction=$txHash"
  }

  object DuplicatedTransaction {
    def apply(t: Transaction) = new DuplicatedTransaction(t.hash)
  }


  case class NoAddressCacheFound(txHash: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which has no address cache"
  }

  object NoAddressCacheFound {
    def apply(t: Transaction) = new NoAddressCacheFound(t.hash)
  }


  case class InsufficientBalance(address: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
  }

  object InsufficientBalance {
    def apply(t: Transaction) = new InsufficientBalance(t.src.address)
  }


  // TODO: pass also a transaction metadata
  case class InternalInconsistency(cbHash: String) extends CheckpointBlockValidation {
    def errorMessage: String = s"CheckpointBlock=$cbHash includes transaction/s which has insufficient balance"
  }

  object InternalInconsistency {
    def apply(cb: CheckpointBlock) = new InternalInconsistency(cb.baseHash)
  }

  sealed trait CheckpointBlockValidatorNel {

    type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]

    def validateTransactionIntegrity(t: Transaction): ValidationResult[Transaction] =
      if (t.valid) t.validNel else InvalidTransaction(t).invalidNel

    def validateSourceAddressCache(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
      dao.addressService
        .get(t.src.address)
        .fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel)

    def validateTransaction(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
      validateTransactionIntegrity(t)
        .product(validateSourceAddressCache(t))
        .map(_ => t)

    def validateTransactions(t: Iterable[Transaction])(implicit dao: DAO): ValidationResult[List[Transaction]] =
      t.toList.map(validateTransaction(_).map(List(_))).combineAll

    def validateDuplicatedTransactions(t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
      val diff = t.toList.diff(t.toSet.toList)

      if (diff.isEmpty) {
        t.toList.validNel
      } else {
        def toError(t: Transaction): ValidationResult[Transaction] = DuplicatedTransaction(t).invalidNel

        diff.map(toError(_).map(List(_))).combineAll
      }
    }

    def validateSignatureIntegrity(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
      if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

    def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
      validateSignatureIntegrity(s, baseHash)
        .map(_ => s)

    def validateSignatures(s: Iterable[HashSignature], baseHash: String): ValidationResult[List[HashSignature]] =
      s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

    def validateEmptySignatures(s: Iterable[HashSignature]): ValidationResult[List[HashSignature]] =
      if (s.nonEmpty) s.toList.validNel else EmptySignatures().invalidNel

    def validateSourceAddressBalances(
      t: Iterable[Transaction]
    )(implicit dao: DAO): ValidationResult[List[Transaction]] = {
      def lookup(key: String) = dao.addressService
        .get(key)
        .map(_.balance)
        .getOrElse(0L)

      def validateBalance(address: String, t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
        val diff = lookup(address) - t.map(_.amount).sum

        if (diff >= 0L) t.toList.validNel else InsufficientBalance(address).invalidNel
      }

      t.toList
        .groupBy(_.src.address)
        .map(a => validateBalance(a._1, a._2))
        .toList
        .combineAll
    }

    type AddressBalance = Map[String, Long]

    def getParents(c: CheckpointBlock)(implicit dao: DAO): List[CheckpointBlock] =
      c.parentSOEBaseHashes
        .toList
        .map(dao.checkpointService.get)
        .map(_.flatMap(_.checkpointBlock))
        .sequence[Option, CheckpointBlock]
        .getOrElse(List())

    def isInSnapshot(c: CheckpointBlock)(implicit dao: DAO): Boolean =
      dao.threadSafeTipService
        .acceptedCBSinceSnapshot
        .contains(c.baseHash)

    def getSummaryBalance(c: CheckpointBlock)(implicit dao: DAO): AddressBalance = {
      val spend = c.transactions
        .groupBy(_.src.address)
        .mapValues(_.map(-_.amount).sum)

      val received = c.transactions
        .groupBy(_.dst.address)
        .mapValues(_.map(_.amount).sum)

      spend |+| received
    }

/*
    def getSnapshotBalances(implicit dao: DAO): AddressBalance =
      dao.threadSafeTipService
        .getSnapshotInfo()
        .addressCacheData
        .mapValues(_.balanceByLatestSnapshot)
*/

    def validateDiff(a: (String, Long))(implicit dao: DAO): Boolean = a match {
      case (hash, diff) => dao.addressService.get(hash).map{_.balanceByLatestSnapshot}.getOrElse(0L) + diff >= 0
    }

    def validateCheckpointBlockTree(cb: CheckpointBlock)(implicit dao: DAO): Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance] =
      if (isInSnapshot(cb)) Map.empty[String, Long].rightIor
      else
        getParents(cb)
          .map(validateCheckpointBlockTree)
          .foldLeft(Map.empty[String, Long].rightIor[NonEmptyList[CheckpointBlockValidation]])((result, d) =>
            result.combine(d))
          .map(getSummaryBalance(cb) |+| _)
          .flatMap(diffs =>
            if (diffs.forall(validateDiff))
              diffs.rightIor
            else
              Ior.both(NonEmptyList.of(InternalInconsistency(cb)), diffs))

    implicit def validateTreeToValidated(v: Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]): ValidationResult[AddressBalance] =
      v match {
        case Ior.Right(a) => a.validNel
        case Ior.Left(a) => a.invalid
        case Ior.Both(a, _) => a.invalid
      }

    def validateCheckpointBlock(
      cb: CheckpointBlock
    )(implicit dao: DAO): ValidationResult[CheckpointBlock] = {
      val preTreeResult =
      validateEmptySignatures(cb.signatures)
        .product(validateSignatures(cb.signatures, cb.baseHash))
        .product(validateTransactions(cb.transactions))
        .product(validateDuplicatedTransactions(cb.transactions))
        .product(validateSourceAddressBalances(cb.transactions))

      val postTreeIgnoreEmptySnapshot = if (dao.threadSafeTipService.lastSnapshotHeight == 0) preTreeResult
      else preTreeResult.product(validateCheckpointBlockTree(cb))

      postTreeIgnoreEmptySnapshot.map(_ => cb)
    }
  }

  object CheckpointBlockValidatorNel extends CheckpointBlockValidatorNel


  case class PeerIPData(canonicalHostName: String, port: Option[Int])
  case class ValidPeerIPData(canonicalHostName: String, port: Int)

  case class GenesisObservation(
                                 genesis: CheckpointBlock,
                                 initialDistribution: CheckpointBlock,
                                 initialDistribution2: CheckpointBlock
                               ) {

    def notGenesisTips(tips: Seq[CheckpointBlock]): Boolean = {
      !tips.contains(initialDistribution) && !tips.contains(initialDistribution2)
    }

  }



  // case class UnresolvedEdge(edge: )

  // TODO: Change options to ResolvedBundleMetaData


  // TODO: Move other messages here.
  sealed trait InternalCommand

  final case object GetPeersID extends InternalCommand
  final case object GetPeersData extends InternalCommand
  final case object GetUTXO extends InternalCommand
  final case object GetValidTX extends InternalCommand
  final case object GetMemPoolUTXO extends InternalCommand
  final case object ToggleHeartbeat extends InternalCommand

  // TODO: Add round to internalheartbeat, would be better than replicating it all over the place

  case class InternalHeartbeat(round: Long = 0L)

  final case object InternalBundleHeartbeat extends InternalCommand

  final case class ValidateTransaction(tx: Transaction) extends InternalCommand

  trait DownloadMessage

  case class DownloadRequest(time: Long = System.currentTimeMillis()) extends DownloadMessage

  final case class SyncData(validTX: Set[Transaction], memPoolTX: Set[Transaction]) extends GossipMessage

  final case class RequestTXProof(txHash: String) extends GossipMessage

  case class MetricsResult(metrics: Map[String, String])

  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(hex: String) {
    def short: String = hex.toString.slice(0, 5)
    def medium: String = hex.toString.slice(0, 10)
    def address: String = KeyUtils.publicKeyToAddressString(toPublicKey)
    def toPublicKey: PublicKey = hexToPublicKey(hex)
  }

  case class Peer(
                   id: Id,
                   externalAddress: Option[InetSocketAddress],
                   apiAddress: Option[InetSocketAddress],
                   remotes: Seq[InetSocketAddress] = Seq(),
                   externalHostString: String
                 ) extends Signable

  case class LocalPeerData(
                            // initialAddAddress: String
                            //    mostRecentSignedPeer: Signed[Peer],
                            //    updatedPeer: Peer,
                            //    lastRXTime: Long = System.currentTimeMillis(),
                            apiClient: APIClient
                          )


  case class Node(address: String, host: String, port: Int)

  case class TransactionSerialized(hash: String, sender: String, receiver: String, amount: Long, signers: Set[String], time: Long)
  object TransactionSerialized {
    def apply(tx: Transaction): TransactionSerialized =
      new TransactionSerialized(tx.hash, tx.src.address, tx.dst.address, tx.amount,
        tx.signatures.map(_.address).toSet, Instant.now.getEpochSecond)
  }

}
