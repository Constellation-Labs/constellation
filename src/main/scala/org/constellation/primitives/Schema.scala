package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.time.Instant

import constellation.pubKeyToAddress
import org.constellation.DAO
import org.constellation.consensus.Consensus.RemoteMessage
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
                            ) extends ProductHash {
    def normalizedBalance: Long = balance / NormalizationFactor
  }

  sealed trait Fiber

  case class TransactionData(
                              src: String,
                              dst: String,
                              amount: Long,
                              salt: Long = Random.nextLong() // To ensure hash uniqueness.
                            ) extends ProductHash with GossipMessage {
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
                              ) extends ProductHash with Fiber

  final case class BundleHash(hash: String) extends Fiber
  final case class TransactionHash(txHash: String) extends Fiber
  final case class ParentBundleHash(pbHash: String) extends Fiber

  // TODO: Make another bundle data with additional metadata for depth etc.
  final case class BundleData(bundles: Seq[Fiber]) extends ProductHash

  case class RequestBundleData(hash: String) extends GossipMessage
  case class HashRequest(hash: String) extends GossipMessage
  case class BatchHashRequest(hashes: Set[String]) extends GossipMessage

  case class BatchBundleHashRequest(hashes: Set[String]) extends GossipMessage with RemoteMessage
  case class BatchTXHashRequest(hashes: Set[String]) extends GossipMessage with RemoteMessage

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
    ValidationHash, BundleDataHash = Value
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
    * @param left : First parent reference in order
    * @param right : Second parent reference
    * @param data : Optional hash reference to attached information
    */
  case class ObservationEdge(
                              left: TypedEdgeHash,
                              right: TypedEdgeHash,
                              data: Option[TypedEdgeHash] = None
                            ) extends ProductHash

  /**
    * Encapsulation for all witness information about a given observation edge.
    * @param signatureBatch : Collection of validation signatures about the edge.
    */
  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends ProductHash {
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
  case class TransactionEdgeData(amount: Long, salt: Long = Random.nextLong()) extends ProductHash

  /**
    * Collection of references to transaction hashes
    * @param hashes : TX edge hashes
    */
  case class CheckpointEdgeData(hashes: Seq[String]) extends ProductHash

  case class ResolvedObservationEdge[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (left: L, right: R, data: Option[D] = None)

  case class Transaction(edge: Edge[Address, Address, TransactionEdgeData]) {

    def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
      //edge.storeTransactionCacheData({originalCache: TransactionCacheData => cache.plus(originalCache)}, cache)
      dao.transactionService.put(this.hash, cache)
    }

    /*def ledgerApplyMemPool(dbActor: Datastore): Unit = {
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
    }*/

    def ledgerApply()(implicit dao: DAO): Unit = {
      dao.addressService.update(
        src.hash,
        { a: AddressCacheData => a.copy(balance = a.balance - amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dao.addressService.update(
        dst.hash,
        { a: AddressCacheData => a.copy(balance = a.balance + amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
    }

    def ledgerApplySnapshot()(implicit dao: DAO): Unit = {
      dao.addressService.update(
        src.hash,
        { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dao.addressService.update(
        dst.hash,
        { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
    }

    def src: Address = edge.resolvedObservationEdge.left
    def dst: Address = edge.resolvedObservationEdge.right

    def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

    // TODO: Add proper exception on empty option
    def amount : Long = edge.resolvedObservationEdge.data.get.amount
    def baseHash: String = edge.signedObservationEdge.baseHash
    def hash: String = edge.signedObservationEdge.hash
    def plus(other: Transaction): Transaction = this.copy(
      edge = edge.plus(other.edge)
    )
    def plus(keyPair: KeyPair): Transaction = this.copy(
      edge = edge.plus(keyPair)
    )

    def valid: Boolean = validSrcSignature &&
      dst.address.nonEmpty &&
      dst.address.length > 30 &&
      dst.address.startsWith("DAG") &&
      amount > 0

    def validSrcSignature: Boolean = {
      edge.signedObservationEdge.signatureBatch.signatures.exists{ hs =>
        hs.publicKey.address == src.address && hs.valid(edge.signedObservationEdge.signatureBatch.hash)
      }
    }

  }

  case class CheckpointEdge(edge: Edge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData]) {
    def plus(other: CheckpointEdge) = this.copy(edge = edge.plus(other.edge))
  }
  case class ValidationEdge(edge: Edge[SignedObservationEdge, SignedObservationEdge, Nothing])

  case class Edge[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (
    observationEdge: ObservationEdge,
    signedObservationEdge: SignedObservationEdge,
    resolvedObservationEdge: ResolvedObservationEdge[L,R,D]
  ) {

    def baseHash: String = signedObservationEdge.signatureBatch.hash
    def parentHashes = Seq(observationEdge.left.hash, observationEdge.right.hash)
    def parents = Seq(observationEdge.left, observationEdge.right)

    def storeTransactionCacheData(db: Datastore, update: TransactionCacheData => TransactionCacheData, empty: TransactionCacheData, resolved: Boolean = false): Unit = {
      db.updateTransactionCacheData(signedObservationEdge.baseHash, update, empty)
      db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
      resolvedObservationEdge.data.foreach {
        data =>
          db.putTransactionEdgeData(data.hash, data.asInstanceOf[TransactionEdgeData])
      }
    }

    def storeCheckpointData(db: Datastore, update: CheckpointCacheData => CheckpointCacheData, empty: CheckpointCacheData, resolved: Boolean = false): Unit = {
      db.updateCheckpointCacheData(signedObservationEdge.baseHash, update, empty)
      db.putSignedObservationEdgeCache(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
      resolvedObservationEdge.data.foreach {
        data =>
          db.putCheckpointEdgeData(data.hash, data.asInstanceOf[CheckpointEdgeData])
      }
    }

    def plus(keyPair: KeyPair): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(keyPair))
    }

    def plus(hs: HashSignature): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(hs))
    }

    def plus(other: Edge[_,_,_]): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(other.signedObservationEdge))
    }

  }

  case class Address(address: String) extends ProductHash {
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

  case class CheckpointBlock(
                              transactions: Seq[Transaction],
                              checkpoint: CheckpointEdge
                            ) {

    def calculateHeight()(implicit dao: DAO): Option[Height] = {

      val parents = parentSOEBaseHashes.map {
        dao.checkpointService.get
      }

      val maxHeight = if (parents.exists(_.isEmpty)) {
        None
      } else {

        val parents2 = parents.map {_.get}
        val heights = parents2.map {_.height.map{_.max}}

        val nonEmptyHeights = heights.flatten
        if (nonEmptyHeights.isEmpty) None else {
          Some(nonEmptyHeights.max + 1)
        }
      }

      val minHeight = if (parents.exists(_.isEmpty)) {
        None
      } else {

        val parents2 = parents.map {_.get}
        val heights = parents2.map {_.height.map{_.min}}

        val nonEmptyHeights = heights.flatten
        if (nonEmptyHeights.isEmpty) None else {
          Some(nonEmptyHeights.min + 1)
        }
      }

      val height = maxHeight.flatMap{ max =>
        minHeight.map{
          min =>
            Height(min, max)
        }
      }

      height

    }

    def transactionsValid: Boolean = transactions.nonEmpty && transactions.forall(_.valid)

    // TODO: Return checkpoint validation status for more info rather than just a boolean
    def simpleValidation(
                          balanceAccessFunction: AddressCacheData => Long = (a: AddressCacheData) => a.balance
                        )(implicit dao: DAO): Boolean = {
      val success = if (!validSignatures || signatures.isEmpty) {
        false
      } else if (!transactionsValid) {
        false
      } else {

        val uniqueCount = transactions.map(_.hash).toSet.size
        if (uniqueCount != transactions.size) {
          return false
        }

        val addressCache = transactions.map {_.src.address}.map { a => a -> dao.addressService.get(a) }
        val cachesFound = addressCache.forall(_._2.nonEmpty)
        if (!cachesFound) {
          false
        } else {

          val sumByAddress = transactions
            .map { t => t.src.address -> t.amount }
            .groupBy(_._1).mapValues(_.map {_._2}.sum)

          val lookup = addressCache.toMap.mapValues(_.get)

          val sufficientBalances = sumByAddress.forall { case (k, v) =>
            balanceAccessFunction(lookup(k)) >= v
          }

          sufficientBalances
        }
      }

      if (success) {
        dao.metricsManager ! IncrementMetric("checkpointValidationSuccess")
      } else {
        dao.metricsManager ! IncrementMetric("checkpointValidationFailure")
      }

      success
    }

    def uniqueSignatures: Boolean = signatures.groupBy(_.b58EncodedPublicKey).forall(_._2.size == 1)

    def signedBy(id: Id) : Boolean = witnessIds.contains(id)

    def hashSignaturesOf(id: Id) : Seq[HashSignature] = signatures.filter(_.toId == id)

    def signatureConflict(other: CheckpointBlock): Boolean = {
      signatures.exists{s =>
        other.signatures.exists{ s2 =>
          s.signature != s2.signature && s.b58EncodedPublicKey == s2.b58EncodedPublicKey
        }
      }
    }

    def witnessIds: Seq[Id] = signatures.map{_.toId}

    def signatures: Seq[HashSignature] = checkpoint.edge.signedObservationEdge.signatureBatch.signatures

    def baseHash: String = checkpoint.edge.baseHash

    def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

    // TODO: Optimize call, should store this value instead of recalculating every time.
    def soeHash: String = checkpoint.edge.signedObservationEdge.hash

    def store(cache: CheckpointCacheData)(implicit dao: DAO): Unit = {
      /*
            transactions.foreach { rt =>
              rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
            }
      */
     // checkpoint.edge.storeCheckpointData(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved)
      dao.checkpointService.put(baseHash, cache)

    }

    def plus(keyPair: KeyPair): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(keyPair)))
    }

    def plus(hs: HashSignature): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(hs)))
    }

    def plus(other: CheckpointBlock): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.plus(other.checkpoint))
    }

    def resolvedOE: ResolvedObservationEdge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData] =
      checkpoint.edge.resolvedObservationEdge

    def parentSOE = Seq(resolvedOE.left, resolvedOE.right)
    def parentSOEHashes = Seq(resolvedOE.left.hash, resolvedOE.right.hash)

    def parentSOEBaseHashes: Seq[String] = parentSOE.map{_.baseHash}

    def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

  }

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

  case class DownloadRequest(time: Long = System.currentTimeMillis()) extends DownloadMessage with RemoteMessage

  final case class SyncData(validTX: Set[Transaction], memPoolTX: Set[Transaction]) extends GossipMessage with RemoteMessage

  final case class RequestTXProof(txHash: String) extends GossipMessage with RemoteMessage

  case class MetricsResult(metrics: Map[String, String])

  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(encodedId: EncodedPublicKey) {
    def short: String = id.toString.slice(15, 20)
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")
    def address: AddressMetaData = pubKeyToAddress(id)
    def b58: String = encodedId.b58Encoded
    def id: PublicKey = encodedId.toPublicKey
  }

  case class HandShake(
                        originPeer: Signed[Peer],
                        destination: InetSocketAddress,
                        peers: Set[Signed[Peer]] = Set(),
                        requestExternalAddressCheck: Boolean = false
                      ) extends ProductHash

  // These exist because type erasure messes up pattern matching on Signed[T] such that
  // you need a wrapper case class like this
  case class HandShakeMessage(handShake: Signed[HandShake]) extends RemoteMessage
  case class HandShakeResponseMessage(handShakeResponse: Signed[HandShakeResponse]) extends RemoteMessage

  case class HandShakeResponse(
                                original: Signed[HandShake],
                                response: HandShake,
                                lastObservedExternalAddress: Option[InetSocketAddress] = None
                              ) extends ProductHash with RemoteMessage

  case class Peer(
                   id: Id,
                   externalAddress: Option[InetSocketAddress],
                   apiAddress: Option[InetSocketAddress],
                   remotes: Seq[InetSocketAddress] = Seq(),
                   externalHostString: String
                 ) extends ProductHash

  case class LocalPeerData(
                            // initialAddAddress: String
                            //    mostRecentSignedPeer: Signed[Peer],
                            //    updatedPeer: Peer,
                            //    lastRXTime: Long = System.currentTimeMillis(),
                            apiClient: APIClient
                          )

  // Experimental below

  case class CounterPartyTXRequest(
                                    dst: AddressMetaData,
                                    counterParty: AddressMetaData,
                                    counterPartyAccount: Option[EncodedPublicKey]
                                  ) extends ProductHash


  final case class ConflictDetectedData(detectedOn: Transaction, conflicts: Seq[Transaction]) extends ProductHash

  final case class ConflictDetected(conflict: Signed[ConflictDetectedData]) extends ProductHash with GossipMessage

  final case class VoteData(accept: Seq[Transaction], reject: Seq[Transaction]) extends ProductHash {
    // used to determine what voting round we are talking about
    def voteRoundHash: String = {
      accept.++(reject).sortBy(t => t.hashCode()).map(f => f.hash).mkString("-")
    }
  }

  final case class Vote(vote: Signed[VoteData]) extends ProductHash with Fiber

  case class TransactionSerialized(hash: String, sender: String, receiver: String, amount: Long, signers: Set[String], time: Long)
  object TransactionSerialized {
    def apply(tx: Transaction): TransactionSerialized =
      new TransactionSerialized(tx.hash, tx.src.address, tx.dst.address, tx.amount,
        tx.signatures.map(_.address).toSet, Instant.now.getEpochSecond)
  }

  case class Node(address: String, host: String, port: Int)

}
