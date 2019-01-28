package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.time.Instant
import cats.data._
import cats.implicits._

import constellation.pubKeyToAddress
import org.constellation.DAO
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.util._

import scala.collection.concurrent.TrieMap
import scala.util.Random

/** Transaction protocol Schema.
  *
  * ??.
  */
object Schema {

  /** ??.
    *
    * @param name     ... ??.
    * @param parent   ... ??.
    * @param children ... ??.
    */
  case class TreeVisual(
                         name: String,
                         parent: String,
                         children: Seq[TreeVisual]
                       )

  /** ??.
    *
    * @param hash        ... identification ??.
    * @param transaction ... transaction instance or None.
    * @param inMemPool   ... Whether transaction is in mempool ??.
    * @param inDAG       ... ??.
    * @param cbEdgeHash  ... ?? or None ??.
    */
  case class TransactionQueryResponse(
                                       hash: String,
                                       transaction: Option[Transaction],
                                       inMemPool: Boolean,
                                       inDAG: Boolean,
                                       cbEdgeHash: Option[String]
                                     )

  /** NodeState ??. */
  object NodeState extends Enumeration {
    type NodeState = Value
    val PendingDownload, DownloadInProgress, DownloadCompleteAwaitingFinalSync, Ready = Value
  }

  /** Transaction validation status trait. */
  sealed trait ValidationStatus

  /** A valid validation status. */
  final case object Valid extends ValidationStatus

  /** A mempool-valid validation status. */
  final case object MempoolValid extends ValidationStatus

  /** A status of unknown validity. */
  final case object Unknown extends ValidationStatus

  /** A double spending validation status. */
  final case object DoubleSpend extends ValidationStatus

  /** Configuration update trait. */
  sealed trait ConfigUpdate

  /** Update to reputation ??.
    *
    * @param updates ... Set of reputation making for the update ??.
    * @todo Confusing naming conventions.
    */
  final case class ReputationUpdates(updates: Seq[UpdateReputation]) extends ConfigUpdate

  /** Update to reputation ??.
    *
    * @param id               ... Identification ??.
    * @param secretReputation ... Non-public/Local reputation value ??.
    * @param publicReputation ... Public reputation value.
    * @todo Confusing naming conventions.
    */
  case class UpdateReputation(id: Id, secretReputation: Option[Double], publicReputation: Option[Double])

  /** Set up multiplication factor.
    *
    * This corresponds to the digits after the dot.
    * The value of 8 is the same as there are 'satoshis' in a BTC.
    */
  val NormalizationFactor: Long = 1e8.toLong

  /** ??.
    *
    * @param dst        ... Receiver destination.
    * @param amount     ... Transaction amount.
    * @param normalized ... Whether to normalize ??.
    */
  case class SendToAddress(
                            dst: String,
                            amount: Long,
                            normalized: Boolean = true
                          ) {

    /** @return Renormalized amount. */
    def amountActual: Long = if (normalized) amount * NormalizationFactor else amount
  }

  /** Account meta data ??.
    *
    * @param address                  ... Accounts addresses string.
    * @param balance                  ... Account balance.
    * @param lastValidTransactionHash ... Last valid transaction on account or None.
    * @param txHashPool               ... Sequence of transaction hashes.
    * @param txHashOverflowPointer    ... ??.
    * @param oneTimeUse               ... ??.
    * @param depth                    ... ??.
    * @todo We also need a hash pointer to represent the post-tx counter party signing data, add later.
    * @todo TX should still be accepted even if metadata is incorrect, it just serves to help validation rounds.
    */
  case class AddressMetaData(
                              address: String,
                              balance: Long = 0L,
                              lastValidTransactionHash: Option[String] = None,
                              txHashPool: Seq[String] = Seq(),
                              txHashOverflowPointer: Option[String] = None,
                              oneTimeUse: Boolean = false,
                              depth: Int = 0
                            ) extends ProductHash {

    /** @return The NormalizationFactor-renormalized balance. */
    def normalizedBalance: Long = balance / NormalizationFactor
  }

  /** Fiber trait ??. */
  sealed trait Fiber

  /** Transaction data.
    *
    * @param src    ... Source of transaction.
    * @param dst    ... Destination of transaction.
    * @param amount ... Transaction amount.
    * @param salt   ... Integer io ensure hash uniqueness.
    */
  case class TransactionData(
                              src: String,
                              dst: String,
                              amount: Long,
                              salt: Long = Random.nextLong()
                            ) extends ProductHash with GossipMessage {

    /** @return "TransactionData" string. */
    def hashType = "TransactionData"

    /** @return The multiplicative inverse of the transaction amount. */
    def inverseAmount: Long = -1 * amount

    /** @return The NormalizationFactor-normalized transaction amount. */
    def normalizedAmount: Long = amount / NormalizationFactor

    /** @return The pretty-printed transaction data. */
    def pretty: String = s"TX: $short FROM: ${src.slice(0, 8)} " +
      s"TO: ${dst.slice(0, 8)} " +
      s"AMOUNT: $normalizedAmount"

    /** Computes the balance of the source ledger.
      *
      * @param ledger ... A ledger ??.
      * @return The input-ledgers balance or 0.
      */
    def srcLedgerBalance(ledger: TrieMap[String, Long]): Long = {
      ledger.getOrElse(src, 0)
    }

    /** Computes the validitify of the source ledger with respect to the transaction.
      *
      * @param ledger ... A ledger ??.
      * @return Whether the source ledgers balances equals or exceeds the transaction amount.
      */
    def ledgerValid(ledger: TrieMap[String, Long]): Boolean = {
      srcLedgerBalance(ledger) >= amount
    }

    /** Updates the ledgers balances.
      *
      * @param ledger ... ??.
      */
    def updateLedger(ledger: TrieMap[String, Long]): Unit = {
      if (ledger.contains(src)) ledger(src) = ledger(src) - amount
      if (ledger.contains(dst)) ledger(dst) = ledger(dst) + amount
      else ledger(dst) = amount
    }

  } // end case class TransactionData

  /** Gossip message trait. */
  sealed trait GossipMessage

  /** BundleBlock ??.
    *
    * @param parentHash ... Hash of the parent.
    * @param height     ... Height of the chain ??.
    * @param txHash     ... Sequence of transaction hashes ??.
    */
  final case class BundleBlock(
                                parentHash: String,
                                height: Long,
                                txHash: Seq[String]
                              ) extends ProductHash with Fiber

  /** Bundle hash ??.
    *
    * @param hash ... Hash as string.
    */
  final case class BundleHash(hash: String) extends Fiber

  /** Transaction hash ??.
    *
    * @param hash ... Hash as string.
    */
  final case class TransactionHash(txHash: String) extends Fiber

  /** Parent bundle hash ??.
    *
    * @param pbHash ... Parent bundle hash as string.
    */
  final case class ParentBundleHash(pbHash: String) extends Fiber

  /** Bundle data ??.
    *
    * @param bundles ... A sequence of fibers.
    * @todo TODO: Make another bundle data with additional metadata for depth etc.
    */
  final case class BundleData(bundles: Seq[Fiber]) extends ProductHash

  /** Request bundle data ??.
    *
    * @param hash ... Hash as string.
    */
  case class RequestBundleData(hash: String) extends GossipMessage

  /** Hash request ??.
    *
    * @param hash ... Hash as string.
    */
  case class HashRequest(hash: String) extends GossipMessage

  /** Batch hash request ??.
    *
    * @param hash ... Hash as string.
    */
  case class BatchHashRequest(hashes: Set[String]) extends GossipMessage

  /** Batch bundle hash request ??.
    *
    * @param hashes ... Set of hashes as strings.
    */
  case class BatchBundleHashRequest(hashes: Set[String]) extends GossipMessage with RemoteMessage

  /** Batch transaction hash request ??.
    *
    * @param hashes ... Set of hashes as strings.
    */
  case class BatchTXHashRequest(hashes: Set[String]) extends GossipMessage with RemoteMessage

  /** Unknown parent hash sync information ??.
    *
    * @param firstRequestTime ... Time of first request ??.
    * @param lastRequestTime  ... Time of last request ??.
    * @param numRequests      ... Number of requests ??.
    */
  case class UnknownParentHashSyncInfo(
                                        firstRequestTime: Long,
                                        lastRequestTime: Long,
                                        numRequests: Int
                                      )

  /** Edge hash type. */
  object EdgeHashType extends Enumeration {
    type EdgeHashType = Value
    val AddressHash,
    CheckpointDataHash, CheckpointHash,
    TransactionDataHash, TransactionHash,
    ValidationHash, BundleDataHash, ChannelMessageDataHash = Value
  }

  /** Bundle edge data ??.
    *
    * @param rank   ... ??.
    * @param hashes ... Sequence of hashes as strings ??.
    */
  case class BundleEdgeData(rank: Double, hashes: Seq[String])

  /** Wrapper for encapsulating a typed hash reference.
    *
    * @param hash     ... String of hashed value.
    * @param hashType ... Strictly typed from set of allowed edge formats.
    */
  case class TypedEdgeHash(hash: String, hashType: EdgeHashType)

  /** Basic edge format for linking two hashes with an optional piece of data attached.
    *
    * Similar to GraphX format. Left is topologically ordered before right.
    *
    * @param left  ... First parent reference in order.
    * @param right ... Second parent reference.
    * @param data  ... Optional hash reference to attached information.
    */
  case class ObservationEdge(
                              left: TypedEdgeHash,
                              right: TypedEdgeHash,
                              data: Option[TypedEdgeHash] = None
                            ) extends ProductHash

  /** Encapsulation for all witness information about a given observation edge.
    *
    * @param signatureBatch ... Collection of validation signatures about the edge.
    */
  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends ProductHash {

    /** Add elements in a key pair.
      *
      * @param keyPair ... pair of keys.
      * @return Value of key pair addition.
      */
    def plus(keyPair: KeyPair): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(keyPair))

    /** Apply signatureBatch plus method to a hash signature.
      *
      * @param hs ... A hash signature.
      * @return A signed observation edge ??.
      */
    def plus(hs: HashSignature): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(hs))

    /** Apply signatureBatch plus method to a signature batch.
      *
      * @param other ... A signature batch.
      * @return A signed observation edge ??.
      */
    def plus(other: SignatureBatch): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other))

    /** Apply signatureBatch plus method to a signed observation edge.
      *
      * @param other ... A signed observation edge.
      * @return A signed observation edge ??.
      */
    def plus(other: SignedObservationEdge): SignedObservationEdge = this.copy(signatureBatch = signatureBatch.plus(other.signatureBatch))

    /** @return The hash of the signature batch. */
    def baseHash: String = signatureBatch.hash

  } // end case class SignedObservationEdge

  /** Holder for ledger update information about a transaction.
    *
    * @param amount ... Quantity to be transferred.
    * @param salt   ... Ensure hash uniqueness.
    */
  case class TransactionEdgeData(amount: Long, salt: Long = Random.nextLong()) extends ProductHash

  /** Collection of references to transaction hashes.
    *
    * @param hashes   ... Transaction edge hashes.
    * @param messages ... Channel messages.
    */
  case class CheckpointEdgeData(hashes: Seq[String], messages: Seq[ChannelMessage] = Seq()) extends ProductHash

  /** Collection of references to transaction hashes.
    *
    * @tparam L ... A ProductHash type.
    * @tparam R ... A ProductHash type.
    * @tparam D ... A ProductHash type.
    * @param left  ... Type L input.
    * @param right ... Type R input.
    * @param data  ... Type D option.
    */
  case class ResolvedObservationEdge[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (left: L, right: R, data: Option[D] = None)

  /** Transaction from edge.
    *
    * @param edge ... An address-address-transactionEdgeData type edge.
    */
  case class Transaction(edge: Edge[Address, Address, TransactionEdgeData]) {

    /** Stores cache in DAO.
      *
      * @todo Remove tmp comment.
      */
    def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
      //edge.storeTransactionCacheData({originalCache: TransactionCacheData => cache.plus(originalCache)}, cache) // tmp comment
      dao.transactionService.put(this.hash, cache)
    }

    /* // tmp comment
    /** ledgerApplyMemPool */
    def ledgerApplyMemPool(dbActor: Datastore): Unit = {
      dbActor.updateAddressCacheData(
        src.hash,
        { a: AddressCacheData => a.copy(memPoolBalance = a.memPoolBalance - amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dbActor.updateAddressCacheData(
        dst.hash,
        { a: AddressCacheData => a.copy(memPoolBalance = a.memPoolBalance + amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
    }
    */

    /** Updates the DAO ??.
      *
      * @param dao ... Data access object.
      */
    def ledgerApply()(implicit dao: DAO): Unit = {
      dao.addressService.update(
        src.hash,
        { a: AddressCacheData => a.copy(balance = a.balance - amount) },
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dao.addressService.update(
        dst.hash,
        { a: AddressCacheData => a.copy(balance = a.balance + amount) },
        AddressCacheData(amount, 0L) // unused since this address should already exist here
      )
    }

    /** Updates the DAO ??.
      *
      * @param dao ... Data access object.
      */
    def ledgerApplySnapshot()(implicit dao: DAO): Unit = {
      dao.addressService.update(
        src.hash,
        { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - amount) },
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dao.addressService.update(
        dst.hash,
        { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + amount) },
        AddressCacheData(amount, 0L) // unused since this address should already exist here
      )
    }

    /** @return Source of resolved observation edge. */
    def src: Address = edge.resolvedObservationEdge.left

    /** @return Destination of resolved observation edge. */
    def dst: Address = edge.resolvedObservationEdge.right

    /** @return Batch signatures. */
    def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

    /** Getter for amount.
      *
      * @return Amount.
      * @todo Add proper exception on empty option.
      */
    def amount: Long = edge.resolvedObservationEdge.data.get.amount

    /** @return Base hash. */
    def baseHash: String = edge.signedObservationEdge.baseHash

    /** @return Hash. */
    def hash: String = edge.signedObservationEdge.hash

    /** Uses plus method of edge on transaction.
      *
      * @param other ... A transaction.
      * @return Addition of edge data in other ??.
      **/
    def plus(other: Transaction): Transaction = this.copy(
      edge = edge.plus(other.edge)
    )

    /** Uses plus method of edge on keyPair.
      *
      * @param keyPair ... A key pair.
      * @return Addition of elements of key pair.
      */
    def plus(keyPair: KeyPair): Transaction = this.copy(
      edge = edge.plus(keyPair)
    )

    /** @return Whether transaction data is valid. */
    def valid: Boolean = validSrcSignature &&
      dst.address.nonEmpty &&
      dst.address.length > 30 &&
      dst.address.startsWith("DAG") &&
      amount > 0

    /** @return Whether the source signature is valid. */
    def validSrcSignature: Boolean = {
      edge.signedObservationEdge.signatureBatch.signatures.exists { hs =>
        hs.publicKey.address == src.address && hs.valid(edge.signedObservationEdge.signatureBatch.hash)
      }
    }

  } // end case class Transaction

  /** Checkpoint edge ??.
    *
    * @param edge ... An edge of type SignedObservationEdge-SignedObservationEdge-CheckpointEdgeData ??.
    */
  case class CheckpointEdge(edge: Edge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData]) {

    /** Uses plus method of edge on a Checkpoint edge.
      *
      * @param other ... A checkpoint edge.
      * @return Addition of edge data in other ??.
      **/
    def plus(other: CheckpointEdge) = this.copy(edge = edge.plus(other.edge))
  }

  /** Validation edge ??.
    *
    * @param edge ... An edge of type SignedObservationEdge-SignedObservationEdge-Nothing ??.
    */
  case class ValidationEdge(edge: Edge[SignedObservationEdge, SignedObservationEdge, Nothing])

  /** Edge ??.
    *
    * @tparam L ... A ProductHash type.
    * @tparam R ... A ProductHash type.
    * @tparam D ... A ProductHash type.
    * @param observationEdge         ... edge ??.
    * @param signedObservationEdge   ... edge ??.
    * @param resolvedObservationEdge ... edge of type depending on L, R and D.
    */
  case class Edge[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (
    observationEdge: ObservationEdge,
    signedObservationEdge: SignedObservationEdge,
    resolvedObservationEdge: ResolvedObservationEdge[L, R, D]
  ) {

    // TODO: See documentation for class above in this file.
    def baseHash: String = signedObservationEdge.signatureBatch.hash

    // TODO: See documentation for class above in this file.
    def parentHashes = Seq(observationEdge.left.hash, observationEdge.right.hash)

    // TODO: See documentation for class above in this file.
    def parents = Seq(observationEdge.left, observationEdge.right)

    // TODO: See documentation for class above in this file.
    def storeTransactionCacheData(db: Datastore, update: TransactionCacheData => TransactionCacheData, empty: TransactionCacheData, resolved: Boolean = false): Unit = {
      db.updateTransactionCacheData(signedObservationEdge.baseHash, update, empty)
      db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
      resolvedObservationEdge.data.foreach {
        data =>
          db.putTransactionEdgeData(data.hash, data.asInstanceOf[TransactionEdgeData])
      }
    }

    // TODO: See documentation for class above in this file.
    def storeCheckpointData(db: Datastore, update: CheckpointCacheData => CheckpointCacheData, empty: CheckpointCacheData, resolved: Boolean = false): Unit = {
      db.updateCheckpointCacheData(signedObservationEdge.baseHash, update, empty)
      db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
      resolvedObservationEdge.data.foreach {
        data =>
          db.putCheckpointEdgeData(data.hash, data.asInstanceOf[CheckpointEdgeData])
      }
    }

    // TODO: See documentation for class above in this file.
    def plus(keyPair: KeyPair): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(keyPair))
    }

    // TODO: See documentation for class above in this file.
    def plus(hs: HashSignature): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(hs))
    }

    // TODO: See documentation for class above in this file.
    def plus(other: Edge[_, _, _]): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(other.signedObservationEdge))
    }

  } // end case class Edge

  // TODO: See documentation for class above in this file.
  case class Address(address: String) extends ProductHash {

    // TODO: See documentation for class above in this file.
    override def hash: String = address
  }

  // TODO: See documentation for class above in this file.
  case class AddressCacheData(
                               balance: Long,
                               memPoolBalance: Long,
                               reputation: Option[Double] = None,
                               ancestorBalances: Map[String, Long] = Map(),
                               ancestorReputations: Map[String, Long] = Map(),
                               //    recentTransactions: Seq[String] = Seq(),
                               balanceByLatestSnapshot: Long = 0L
                             ) {

    // TODO: See documentation for class above in this file.
    def plus(previous: AddressCacheData): AddressCacheData = {
      this.copy(
        ancestorBalances =
          ancestorBalances ++ previous.ancestorBalances.filterKeys(k => !ancestorBalances.contains(k)),
        ancestorReputations =
          ancestorReputations ++ previous.ancestorReputations.filterKeys(k => !ancestorReputations.contains(k))
        //recentTransactions =
        //  recentTransactions ++ previous.recentTransactions.filter(k => !recentTransactions.contains(k)) // tmp comment
      )
    }

  }

  /* // tmp comment
  // Instead of one balance we need a Map from soe hash to balance and reputation
  // These values should be removed automatically by eviction
  // We can maintain some kind of automatic LRU cache for keeping track of what we want to remove
  // override evict method, and clean up data.
  // We should also mark a given balance / rep as the 'primary' one.
  */

  // TODO: See documentation for class above in this file.
  case class TransactionCacheData(
                                   transaction: Transaction,
                                   valid: Boolean = false,
                                   inMemPool: Boolean = false,
                                   inDAG: Boolean = false,
                                   inDAGByAncestor: Map[String, Boolean] = Map(),
                                   resolved: Boolean = false,
                                   cbBaseHash: Option[String] = None,
                                   cbForkBaseHashes: Set[String] = Set(),
                                   signatureForks: Set[Transaction] = Set(),
                                   rxTime: Long = System.currentTimeMillis()
                                 ) {

    // TODO: See documentation for class above in this file.
    def plus(previous: TransactionCacheData): TransactionCacheData = {
      this.copy(
        inDAGByAncestor = inDAGByAncestor ++ previous.inDAGByAncestor.filterKeys(k => !inDAGByAncestor.contains(k)),
        cbForkBaseHashes = (cbForkBaseHashes ++ previous.cbForkBaseHashes) -- cbBaseHash.map { s => Set(s) }.getOrElse(Set()),
        signatureForks = (signatureForks ++ previous.signatureForks) - transaction,
        rxTime = previous.rxTime
      )
    }
  }

  // TODO: See documentation for class above in this file.
  case class Height(min: Long, max: Long)

  // TODO: See documentation for class above in this file.
  case class CommonMetadata(
                             valid: Boolean = true,
                             inDAG: Boolean = false,
                             resolved: Boolean = true,
                             resolutionInProgress: Boolean = false,
                             inMemPool: Boolean = false,
                             lastResolveAttempt: Option[Long] = None,
                             rxTime: Long = System.currentTimeMillis(), // TODO: Unify common metadata like this
                           )

  // TODO: Separate cache with metadata vs what is stored in snapshot. // tmp comment

  // TODO: See documentation for class above in this file.
  case class CheckpointCacheData(
                                  checkpointBlock: Option[CheckpointBlock] = None,
                                  //         metadata: CommonMetadata = CommonMetadata(), // tmp comment
                                  //         children: Set[String] = Set(), // tmp comment
                                  height: Option[Height] = None
                                ) {
    /*
        // TODO: See documentation for class above in this file.
        def plus(previous: CheckpointCacheData): CheckpointCacheData = {
          this.copy(
            lastResolveAttempt = lastResolveAttempt.map{t => Some(t)}.getOrElse(previous.lastResolveAttempt),
            rxTime = previous.rxTime
          )
        }
    */
  }

  // TODO: See documentation for class above in this file.
  case class SignedObservationEdgeCache(signedObservationEdge: SignedObservationEdge, resolved: Boolean = false)

  // TODO: See documentation for class above in this file.
  sealed trait CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String
  }

  // TODO: See documentation for class above in this file.
  case class EmptySignatures() extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = "CheckpointBlock has no signatures"
  }

  // TODO: See documentation for class above in this file.
  case class InvalidSignature(signature: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock includes signature=$signature which is invalid"
  }

  // TODO: See documentation for class above in this file.
  object InvalidSignature {

    // TODO: See documentation for class above in this file.
    def apply(s: HashSignature) = new InvalidSignature(s.signature)
  }

  // TODO: See documentation for class above in this file.
  case class InvalidTransaction(txHash: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which is invalid"
  }

  // TODO: See documentation for class above in this file.
  object InvalidTransaction {

    // TODO: See documentation for class above in this file.
    def apply(t: Transaction) = new InvalidTransaction(t.hash)
  }

  // TODO: See documentation for class above in this file.
  case class DuplicatedTransaction(txHash: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock includes duplicated transaction=$txHash"
  }

  // TODO: See documentation for class above in this file.
  object DuplicatedTransaction {

    // TODO: See documentation for class above in this file.
    def apply(t: Transaction) = new DuplicatedTransaction(t.hash)
  }

  // TODO: See documentation for class above in this file.
  case class NoAddressCacheFound(txHash: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock includes transaction=$txHash which has no address cache"
  }

  // TODO: See documentation for class above in this file.
  object NoAddressCacheFound {

    // TODO: See documentation for class above in this file.
    def apply(t: Transaction) = new NoAddressCacheFound(t.hash)
  }

  // TODO: See documentation for class above in this file.
  case class InsufficientBalance(address: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock includes transaction from address=$address which has insufficient balance"
  }

  // TODO: See documentation for class above in this file.
  object InsufficientBalance {

    // TODO: See documentation for class above in this file.
    def apply(t: Transaction) = new InsufficientBalance(t.src.address)
  }

  // TODO: pass also a transaction metadata // tmp comment

  // TODO: See documentation for class above in this file.
  case class InternalInconsistency(cbHash: String) extends CheckpointBlockValidation {

    // TODO: See documentation for class above in this file.
    def errorMessage: String = s"CheckpointBlock=$cbHash includes transaction/s which has insufficient balance"
  }

  // TODO: See documentation for class above in this file.
  object InternalInconsistency {

    // TODO: See documentation for class above in this file.
    def apply(cb: CheckpointBlock) = new InternalInconsistency(cb.baseHash)
  }

  // TODO: See documentation for class above in this file.
  sealed trait CheckpointBlockValidatorNel {

    type ValidationResult[A] = ValidatedNel[CheckpointBlockValidation, A]

    // TODO: See documentation for class above in this file.
    def validateTransactionIntegrity(t: Transaction): ValidationResult[Transaction] =
      if (t.valid) t.validNel else InvalidTransaction(t).invalidNel

    // TODO: See documentation for class above in this file.
    def validateSourceAddressCache(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
      dao.addressService
        .get(t.src.address)
        .fold[ValidationResult[Transaction]](NoAddressCacheFound(t).invalidNel)(_ => t.validNel)

    // TODO: See documentation for class above in this file.
    def validateTransaction(t: Transaction)(implicit dao: DAO): ValidationResult[Transaction] =
      validateTransactionIntegrity(t)
        .product(validateSourceAddressCache(t))
        .map(_ => t)

    // TODO: See documentation for class above in this file.
    def validateTransactions(t: Iterable[Transaction])(implicit dao: DAO): ValidationResult[List[Transaction]] =
      t.toList.map(validateTransaction(_).map(List(_))).combineAll

    // TODO: See documentation for class above in this file.
    def validateDuplicatedTransactions(t: Iterable[Transaction]): ValidationResult[List[Transaction]] = {
      val diff = t.toList.diff(t.toSet.toList)

      if (diff.isEmpty) {
        t.toList.validNel
      } else {

        // TODO: See documentation for class above in this file.
        def toError(t: Transaction): ValidationResult[Transaction] = DuplicatedTransaction(t).invalidNel

        diff.map(toError(_).map(List(_))).combineAll
      }
    }

    // TODO: See documentation for class above in this file.
    def validateSignatureIntegrity(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
      if (s.valid(baseHash)) s.validNel else InvalidSignature(s).invalidNel

    // TODO: See documentation for class above in this file.
    def validateSignature(s: HashSignature, baseHash: String): ValidationResult[HashSignature] =
      validateSignatureIntegrity(s, baseHash)
        .map(_ => s)

    // TODO: See documentation for class above in this file.
    def validateSignatures(s: Iterable[HashSignature], baseHash: String): ValidationResult[List[HashSignature]] =
      s.toList.map(validateSignature(_, baseHash).map(List(_))).combineAll

    // TODO: See documentation for class above in this file.
    def validateEmptySignatures(s: Iterable[HashSignature]): ValidationResult[List[HashSignature]] =
      if (s.nonEmpty) s.toList.validNel else EmptySignatures().invalidNel

    // TODO: See documentation for class above in this file.
    def validateSourceAddressBalances(
                                       t: Iterable[Transaction],
                                       balanceAccessFunction: AddressCacheData => Long,
                                     )(implicit dao: DAO): ValidationResult[List[Transaction]] = {

      // TODO: See documentation for class above in this file.
      def lookup(key: String) = dao.addressService
        .get(key)
        .map(_.balance)
        .getOrElse(0L)

      // TODO: See documentation for class above in this file.
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

    // TODO: See documentation for class above in this file.
    def getParents(c: CheckpointBlock)(implicit dao: DAO): List[CheckpointBlock] =
      c.parentSOEBaseHashes
        .toList
        .map(dao.checkpointService.get)
        .map(_.flatMap(_.checkpointBlock))
        .sequence[Option, CheckpointBlock]
        .getOrElse(List())

    // TODO: See documentation for class above in this file.
    def isInSnapshot(c: CheckpointBlock)(implicit dao: DAO): Boolean =
      dao.threadSafeTipService
        .acceptedCBSinceSnapshot
        .contains(c.baseHash)

    // TODO: See documentation for class above in this file.
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
        // TODO: See documentation for class above in this file.
        def getSnapshotBalances(implicit dao: DAO): AddressBalance =
          dao.threadSafeTipService
            .getSnapshotInfo()
            .addressCacheData
            .mapValues(_.balanceByLatestSnapshot)
    */

    // TODO: See documentation for class above in this file.
    def validateDiff(a: (String, Long))(implicit dao: DAO): Boolean = a match {
      case (hash, diff) => dao.addressService.get(hash).map {
        _.balanceByLatestSnapshot
      }.getOrElse(0L) + diff >= 0
    }

    // TODO: See documentation for class above in this file.
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

    // TODO: See documentation for class above in this file.
    implicit def validateTreeToValidated(v: Ior[NonEmptyList[CheckpointBlockValidation], AddressBalance]): ValidationResult[AddressBalance] =
      v match {
        case Ior.Right(a) => a.validNel
        case Ior.Left(a) => a.invalid
        case Ior.Both(a, _) => a.invalid
      }

    // TODO: See documentation for class above in this file.
    def validateCheckpointBlock(
                                 cb: CheckpointBlock,
                                 balanceAccessFunction: AddressCacheData => Long,
                               )(implicit dao: DAO): ValidationResult[CheckpointBlock] = {
      val preTreeResult =
        validateEmptySignatures(cb.signatures)
          .product(validateSignatures(cb.signatures, cb.baseHash))
          .product(validateTransactions(cb.transactions))
          .product(validateDuplicatedTransactions(cb.transactions))
          .product(validateSourceAddressBalances(cb.transactions, balanceAccessFunction))

      val postTreeIgnoreEmptySnapshot = if (dao.threadSafeTipService.lastSnapshotHeight == 0) preTreeResult
      else preTreeResult.product(validateCheckpointBlockTree(cb))

      postTreeIgnoreEmptySnapshot.map(_ => cb)
    }

  } // end sealed trait CheckpointBlockValidatorNel

  // TODO: See documentation for class above in this file.
  object CheckpointBlockValidatorNel extends CheckpointBlockValidatorNel

  // TODO: See documentation for class above in this file.
  case class CheckpointBlock(
                              transactions: Seq[Transaction],
                              checkpoint: CheckpointEdge
                            ) {

    // TODO: See documentation for class above in this file.
    def calculateHeight()(implicit dao: DAO): Option[Height] = {

      val parents = parentSOEBaseHashes.map {
        dao.checkpointService.get
      }

      val maxHeight = if (parents.exists(_.isEmpty)) {
        None
      } else {

        val parents2 = parents.map {
          _.get
        }
        val heights = parents2.map {
          _.height.map {
            _.max
          }
        }

        val nonEmptyHeights = heights.flatten
        if (nonEmptyHeights.isEmpty) None else {
          Some(nonEmptyHeights.max + 1)
        }
      }

      val minHeight = if (parents.exists(_.isEmpty)) {
        None
      } else {

        val parents2 = parents.map {
          _.get
        }
        val heights = parents2.map {
          _.height.map {
            _.min
          }
        }

        val nonEmptyHeights = heights.flatten
        if (nonEmptyHeights.isEmpty) None else {
          Some(nonEmptyHeights.min + 1)
        }
      }

      val height = maxHeight.flatMap { max =>
        minHeight.map {
          min =>
            Height(min, max)
        }
      }

      height

    }

    // TODO: See documentation for class above in this file.
    def transactionsValid: Boolean = transactions.nonEmpty && transactions.forall(_.valid)

    // TODO: Return checkpoint validation status for more info rather than just a boolean // tmp comment

    // TODO: See documentation for class above in this file.
    def simpleValidation(
                          balanceAccessFunction: AddressCacheData => Long = (a: AddressCacheData) => a.balance
                        )(implicit dao: DAO): Boolean = {

      val validation = CheckpointBlockValidatorNel.validateCheckpointBlock(
        CheckpointBlock(transactions, checkpoint),
        balanceAccessFunction
      )

      if (validation.isValid) {
        dao.metricsManager ! IncrementMetric("checkpointValidationSuccess")
      } else {
        dao.metricsManager ! IncrementMetric("checkpointValidationFailure")
      }

      // TODO: Return Validation instead of Boolean // tmp comment

      validation.isValid
    }

    // TODO: See documentation for class above in this file.
    def uniqueSignatures: Boolean = signatures.groupBy(_.b58EncodedPublicKey).forall(_._2.size == 1)

    // TODO: See documentation for class above in this file.
    def signedBy(id: Id): Boolean = witnessIds.contains(id)

    // TODO: See documentation for class above in this file.
    def hashSignaturesOf(id: Id): Seq[HashSignature] = signatures.filter(_.toId == id)

    // TODO: See documentation for class above in this file.
    def signatureConflict(other: CheckpointBlock): Boolean = {
      signatures.exists { s =>
        other.signatures.exists { s2 =>
          s.signature != s2.signature && s.b58EncodedPublicKey == s2.b58EncodedPublicKey
        }
      }
    }

    // TODO: See documentation for class above in this file.
    def witnessIds: Seq[Id] = signatures.map {
      _.toId
    }

    // TODO: See documentation for class above in this file.
    def signatures: Seq[HashSignature] = checkpoint.edge.signedObservationEdge.signatureBatch.signatures

    // TODO: See documentation for class above in this file.
    def baseHash: String = checkpoint.edge.baseHash

    // TODO: See documentation for class above in this file.
    def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

    // TODO: Optimize call, should store this value instead of recalculating every time. // tmp comment

    // TODO: See documentation for class above in this file.
    def soeHash: String = checkpoint.edge.signedObservationEdge.hash

    // TODO: See documentation for class above in this file.
    def store(cache: CheckpointCacheData)(implicit dao: DAO): Unit = {
      /* // tmp comment
            transactions.foreach { rt =>
              rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
            }
      */

      // checkpoint.edge.storeCheckpointData(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved) // tmp comment

      dao.checkpointService.put(baseHash, cache)
    }

    // TODO: See documentation for class above in this file.
    def plus(keyPair: KeyPair): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(keyPair)))
    }

    // TODO: See documentation for class above in this file.
    def plus(hs: HashSignature): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(hs)))
    }

    // TODO: See documentation for class above in this file.
    def plus(other: CheckpointBlock): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.plus(other.checkpoint))
    }

    // TODO: See documentation for class above in this file.
    def resolvedOE: ResolvedObservationEdge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData] =
      checkpoint.edge.resolvedObservationEdge

    // TODO: See documentation for class above in this file.
    def parentSOE = Seq(resolvedOE.left, resolvedOE.right)

    // TODO: See documentation for class above in this file.
    def parentSOEHashes = Seq(resolvedOE.left.hash, resolvedOE.right.hash)

    // TODO: See documentation for class above in this file.
    def parentSOEBaseHashes: Seq[String] = parentSOE.map(h => if (h == null) "" else h.baseHash)

    // TODO: See documentation for class above in this file.
    def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

  } // end case class CheckpointBlock

  // TODO: See documentation for class above in this file.
  case class PeerIPData(canonicalHostName: String, port: Option[Int])

  // TODO: See documentation for class above in this file.
  case class ValidPeerIPData(canonicalHostName: String, port: Int)

  // TODO: See documentation for class above in this file.
  case class GenesisObservation(
                                 genesis: CheckpointBlock,
                                 initialDistribution: CheckpointBlock,
                                 initialDistribution2: CheckpointBlock
                               ) {

    // TODO: See documentation for class above in this file.
    def notGenesisTips(tips: Seq[CheckpointBlock]): Boolean = {
      !tips.contains(initialDistribution) && !tips.contains(initialDistribution2)
    }

  }

  // case class UnresolvedEdge(edge: ) // tmp comment

  // TODO: Change options to ResolvedBundleMetaData // tmp comment

  // TODO: Move other messages here. // tmp comment

  // TODO: See documentation for class above in this file.
  sealed trait InternalCommand

  // TODO: See documentation for class above in this file.
  final case object GetPeersID extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case object GetPeersData extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case object GetUTXO extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case object GetValidTX extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case object GetMemPoolUTXO extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case object ToggleHeartbeat extends InternalCommand

  // TODO: Add round to internalheartbeat, would be better than replicating it all over the place

  // TODO: See documentation for class above in this file.
  case class InternalHeartbeat(round: Long = 0L)

  // TODO: See documentation for class above in this file.
  final case object InternalBundleHeartbeat extends InternalCommand

  // TODO: See documentation for class above in this file.
  final case class ValidateTransaction(tx: Transaction) extends InternalCommand

  // TODO: See documentation for class above in this file.
  trait DownloadMessage

  // TODO: See documentation for class above in this file.
  case class DownloadRequest(time: Long = System.currentTimeMillis()) extends DownloadMessage with RemoteMessage

  // TODO: See documentation for class above in this file.
  final case class SyncData(validTX: Set[Transaction], memPoolTX: Set[Transaction]) extends GossipMessage with RemoteMessage

  // TODO: See documentation for class above in this file.
  final case class RequestTXProof(txHash: String) extends GossipMessage with RemoteMessage

  // TODO: See documentation for class above in this file.
  case class MetricsResult(metrics: Map[String, String])

  // TODO: See documentation for class above in this file.
  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand

  // TODO: See documentation for class above in this file.
  case class Peers(peers: Seq[InetSocketAddress])

  // TODO: See documentation for class above in this file.

  /** Wrapper class for the public key.
    *
    * Gives access to associated public key and address.
    *
    * @param encodedId ... Underlying public key.
    */
  case class Id(encodedId: EncodedPublicKey) {

    // TODO: See documentation for class above in this file.
    def short: String = id.toString.slice(15, 20)

    // TODO: See documentation for class above in this file.
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")

    // TODO: See documentation for class above in this file.
    def address: AddressMetaData = pubKeyToAddress(id)

    /** @return The base 58 encoded variant of the private key. */
    def b58: String = encodedId.b58Encoded

    /** @return The public key. */
    def id: PublicKey = encodedId.toPublicKey

  }

  // TODO: See documentation for class above in this file.
  case class HandShake(
                        originPeer: Signed[Peer],
                        destination: InetSocketAddress,
                        peers: Set[Signed[Peer]] = Set(),
                        requestExternalAddressCheck: Boolean = false
                      ) extends ProductHash

  /* // tmp coment
  These exist because type erasure messes up pattern matching on Signed[T] such that
  you need a wrapper case class like this
  */

  // TODO: See documentation for class above in this file.
  case class HandShakeMessage(handShake: Signed[HandShake]) extends RemoteMessage

  // TODO: See documentation for class above in this file.
  case class HandShakeResponseMessage(handShakeResponse: Signed[HandShakeResponse]) extends RemoteMessage

  // TODO: See documentation for class above in this file.
  case class HandShakeResponse(
                                original: Signed[HandShake],
                                response: HandShake,
                                lastObservedExternalAddress: Option[InetSocketAddress] = None
                              ) extends ProductHash with RemoteMessage

  // TODO: See documentation for class above in this file.
  case class Peer(
                   id: Id,
                   externalAddress: Option[InetSocketAddress],
                   apiAddress: Option[InetSocketAddress],
                   remotes: Seq[InetSocketAddress] = Seq(),
                   externalHostString: String
                 ) extends ProductHash

  // TODO: See documentation for class above in this file.
  /** @todo: Remove tmp comment. */
  case class LocalPeerData(
                            // initialAddAddress: String // tmp comment
                            //    mostRecentSignedPeer: Signed[Peer], // tmp comment
                            //    updatedPeer: Peer, // tmp comment
                            //    lastRXTime: Long = System.currentTimeMillis(), // tmp comment
                            apiClient: APIClient
                          )

  // Experimental below // tmp comment

  // TODO: See documentation for class above in this file.
  case class CounterPartyTXRequest(
                                    dst: AddressMetaData,
                                    counterParty: AddressMetaData,
                                    counterPartyAccount: Option[EncodedPublicKey]
                                  ) extends ProductHash

  // TODO: See documentation for class above in this file.
  final case class ConflictDetectedData(detectedOn: Transaction, conflicts: Seq[Transaction]) extends ProductHash

  // TODO: See documentation for class above in this file.
  final case class ConflictDetected(conflict: Signed[ConflictDetectedData]) extends ProductHash with GossipMessage

  // TODO: See documentation for class above in this file.
  final case class VoteData(accept: Seq[Transaction], reject: Seq[Transaction]) extends ProductHash {

    // used to determine what voting round we are talking about // tmp comment

    // TODO: See documentation for class above in this file.
    def voteRoundHash: String = {
      accept.++(reject).sortBy(t => t.hashCode()).map(f => f.hash).mkString("-")
    }
  }

  // TODO: See documentation for class above in this file.
  final case class Vote(vote: Signed[VoteData]) extends ProductHash with Fiber

  /** Wrapper for serialized transaction case class.
    *
    * @param hash     ... ? string.
    * @param sender   ... Sender identifier.
    * @param receiver ... Receiver identifier.
    * @param amount   ... Transaction amount.
    * @param signers  ... Set of signer identifiers.
    * @param time     ... Timestamp.
    */
  case class TransactionSerialized(hash: String, sender: String, receiver: String,
                                   amount: Long, signers: Set[String], time: Long)

  /** Wrapper for serialized transaction companion object. */
  object TransactionSerialized {

    /** Apply method.
      *
      * @param tx ... Transaction to be wrapped into an TransactionSerialized instance.
      * @return Serialized transaction.
      */
    def apply(tx: Transaction): TransactionSerialized =
      new TransactionSerialized(tx.hash, tx.src.address, tx.dst.address, tx.amount,
        tx.signatures.map(_.address).toSet, Instant.now.getEpochSecond)
  }

  /** Node case class.
    *
    * @param address ... Address string.
    * @param host    ... Host identifier.
    * @param port    ... Port number integer.
    */
  case class Node(address: String, host: String, port: Int)

} // end object Schema
