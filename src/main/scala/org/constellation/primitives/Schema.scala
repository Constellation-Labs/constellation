package org.constellation.primitives

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import akka.actor.ActorRef
import constellation.pubKeyToAddress
import org.constellation.LevelDB.{DBPut, DBUpdate}
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.crypto.Base58
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.util._

import scala.collection.concurrent.TrieMap
import scala.collection.{SortedSet, mutable}
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

  sealed trait NodeState
  final case object PendingDownload extends NodeState
  final case object Ready extends NodeState

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

  case class BundleHashQueryResponse(hash: String, sheaf: Option[Sheaf], transactions: Seq[Transaction])

  case class MaxBundleGenesisHashQueryResponse(genesisBundle: Option[Bundle], genesisTX: Option[Transaction], sheaf: Option[Sheaf])

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
    ValidationHash = Value
  }

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

  case class EdgeCell(members: mutable.SortedSet[EdgeSheaf])

  case class Transaction(edge: Edge[Address, Address, TransactionEdgeData]) {

    def store(dbActor: ActorRef, cache: TransactionCacheData): Unit = {
      edge.store(dbActor, {originalCache: TransactionCacheData => cache.plus(originalCache)}, cache)
    }

    def ledgerApplyMemPool(dbActor: ActorRef): Unit = {
      dbActor ! DBUpdate(
        src.hash,
        { a: AddressCacheData => a.copy(memPoolBalance = a.memPoolBalance - amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dbActor ! DBUpdate(
        dst.hash,
        { a: AddressCacheData => a.copy(memPoolBalance = a.memPoolBalance + amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
    }

    def ledgerApply(dbActor: ActorRef): Unit = {
      dbActor ! DBUpdate(
        src.hash,
        { a: AddressCacheData => a.copy(balance = a.balance - amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
      dbActor ! DBUpdate(
        dst.hash,
        { a: AddressCacheData => a.copy(balance = a.balance + amount)},
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

    def store[T <: AnyRef](db: ActorRef, update: T => T, empty: T, resolved: Boolean = false): Unit = {
      db ! DBUpdate(signedObservationEdge.baseHash, update, empty)
      db ! DBPut(signedObservationEdge.hash, SignedObservationEdgeCache(signedObservationEdge, resolved))
      storeData(db: ActorRef)
    }

    def storeData(db: ActorRef): Unit = {
      resolvedObservationEdge.data.foreach {
        data =>
          db ! DBPut(data.hash, data)
      }
    }

    def plus(keyPair: KeyPair): Edge[L, R, D] = {
      this.copy(signedObservationEdge = signedObservationEdge.plus(keyPair))
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
                               recentTransactions: Seq[String] = Seq()
                             ) {

    def plus(previous: AddressCacheData): AddressCacheData = {
      this.copy(
        ancestorBalances =
          ancestorBalances ++ previous.ancestorBalances.filterKeys(k => !ancestorBalances.contains(k)),
        ancestorReputations =
          ancestorReputations ++ previous.ancestorReputations.filterKeys(k => !ancestorReputations.contains(k)),
        recentTransactions =
          recentTransactions ++ previous.recentTransactions.filter(k => !recentTransactions.contains(k))
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

  case class CheckpointCacheData(
                                  checkpointBlock: CheckpointBlock,
                                  inDAG: Boolean = false,
                                  resolved: Boolean = false,
                                  resolutionInProgress: Boolean = false,
                                  inMemPool: Boolean = false,
                                  lastResolveAttempt: Option[Long] = None,
                                  rxTime: Long = System.currentTimeMillis(), // TODO: Unify common metadata like this
                                  children: Set[String] = Set(),
                                  forkChildren: Set[String] = Set()
                                ) {

    def plus(previous: CheckpointCacheData): CheckpointCacheData = {
      this.copy(
        lastResolveAttempt = lastResolveAttempt.map{t => Some(t)}.getOrElse(previous.lastResolveAttempt),
        rxTime = previous.rxTime
      )
    }

  }

  case class SignedObservationEdgeCache(signedObservationEdge: SignedObservationEdge, resolved: Boolean = false)

  case class CheckpointBlock(
                              transactions: Seq[Transaction],
                              checkpoint: CheckpointEdge
                                  ) {

    def signatures: Seq[HashSignature] = checkpoint.edge.signedObservationEdge.signatureBatch.signatures

    def baseHash: String = checkpoint.edge.baseHash

    // TODO: Optimize call, should store this value instead of recalculating every time.
    def soeHash: String = checkpoint.edge.signedObservationEdge.hash

    def store(db: ActorRef, cache: CheckpointCacheData, resolved: Boolean): Unit = {
/*
      transactions.foreach { rt =>
        rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
      }
*/
      checkpoint.edge.store(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved)
    }

    def plus(keyPair: KeyPair): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(keyPair)))
    }

    def plus(other: CheckpointBlock): CheckpointBlock = {
      this.copy(checkpoint = checkpoint.plus(other.checkpoint))
    }

    def resolvedOE: ResolvedObservationEdge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData] =
      checkpoint.edge.resolvedObservationEdge

    def parentSOE = Seq(resolvedOE.left, resolvedOE.right)

    def parentSOEBaseHashes: Seq[String] = parentSOE.map{_.baseHash}

  }

  case class EdgeSheaf(
                        signedObservationEdge: SignedObservationEdge,
                        parent: String,
                        height: Long,
                        depth: Int,
                        score: Double
                      )


  case class PeerIPData(canonicalHostName: String, port: Option[Int])
  case class ValidPeerIPData(canonicalHostName: String, port: Int)

  case class GenesisObservation(
                                 genesis: CheckpointBlock,
                                 initialDistribution: CheckpointBlock,
                                 initialDistribution2: CheckpointBlock
                               )



  // case class UnresolvedEdge(edge: )

  // TODO: Change options to ResolvedBundleMetaData


  /**
    * Local neighborhood data about a bundle
    * @param bundle : The raw bundle as embedded in the chain
    * @param height : Main Chain height
    * @param reputations : Raw heuristic score calculated from previous parent bundle
    * @param totalScore : Total score of entire chain including this bundle
    * @param rxTime : Local node receipt time
    * @param transactionsResolved : Whether or not the transaction hashes have been resolved.
    */
  case class Sheaf(
                    bundle: Bundle,
                    height: Option[Int] = None,
                    reputations: Map[String, Long] = Map(),
                    totalScore: Option[Double] = None,
                    rxTime: Long = System.currentTimeMillis(),
                    transactionsResolved: Boolean = false,
                    parent: Option[Bundle] = None,
                    child: Option[Bundle] = None,
                    pendingChildren: Option[Seq[Bundle]] = None,
                    neighbors: Option[Seq[Sheaf]] = None
                  ) {
    def safeBundle = Option(bundle)
    def isResolved: Boolean = reputations.nonEmpty && transactionsResolved && height.nonEmpty && totalScore.nonEmpty
    def cellKey: CellKey = CellKey(bundle.extractParentBundleHash.pbHash, bundle.maxStackDepth, height.getOrElse(0))
  }

  case class CellKey(hashPointer: String, depth: Int, height: Int)

  case class Cell(members: SortedSet[Sheaf])

  final case class PeerSyncHeartbeat(
                                      maxBundleMeta: Sheaf,
                                      validLedger: Map[String, Long],
                                      id: Id
                                    ) extends GossipMessage with RemoteMessage {
    def safeMaxBundle = Option(maxBundle)
    def maxBundle: Bundle = maxBundleMeta.bundle
  }

  final case class Bundle(
                           bundleData: Signed[BundleData]
                         ) extends ProductHash with Fiber with GossipMessage
    with RemoteMessage {

    def extractTXHash: Set[TransactionHash] = {
      def process(s: Signed[BundleData]): Set[TransactionHash] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            process(b2.bundleData)
          case h: TransactionHash => Set(h)
          case _ => Set[TransactionHash]()
        }
        if (depths.nonEmpty) {
          depths.reduce( (s1: Set[TransactionHash], s2: Set[TransactionHash]) => s1 ++ s2)
        } else {
          Set[TransactionHash]()
        }
      }
      process(bundleData)
    }

    val bundleNumber: Long = 0L //Random.nextLong()

    /*
        def extractTreeVisual: TreeVisual = {
          val parentHash = extractParentBundleHash.hash.slice(0, 5)
          def process(s: Signed[BundleData], parent: String): Seq[TreeVisual] = {
            val bd = s.data.bundles
            val depths = bd.map {
              case tx: TX =>
                TreeVisual(s"TX: ${tx.short} amount: ${tx.tx.data.normalizedAmount}", parent, Seq())
              case b2: Bundle =>
                val name = s"Bundle: ${b2.short} numTX: ${b2.extractTX.size}"
                val children = process(b2.bundleData, name)
                TreeVisual(name, parent, children)
              case _ => Seq()
            }
            depths
          }.asInstanceOf[Seq[TreeVisual]]
          val parentName = s"Bundle: $short numTX: ${extractTX.size} node: ${bundleData.id.short} parent: $parentHash"
          val children = process(bundleData, parentName)
          TreeVisual(parentName, "null", children)
        }
    */

    def extractParentBundleHash: ParentBundleHash = {
      def process(s: Signed[BundleData]): ParentBundleHash = {
        val bd = s.data.bundles
        val depths = bd.collectFirst{
          case b2: Bundle =>
            process(b2.bundleData)
          case bh: ParentBundleHash => bh
        }
        depths.get
      }
      process(bundleData)
    }

    def extractBundleHashId: (BundleHash, Id) = {
      def process(s: Signed[BundleData]): (BundleHash, Id) = {
        val bd = s.data.bundles
        val depths = bd.collectFirst{
          case b2: Bundle =>
            process(b2.bundleData)
          case bh: BundleHash => bh -> s.id
        }.get
        depths
      }
      process(bundleData)
    }

    def extractSubBundlesMinSize(minSize: Int = 2) = {
      extractSubBundles.filter{_.maxStackDepth >= minSize}
    }

    def extractSubBundleHashes: Set[String] = {
      def process(s: Signed[BundleData]): Set[String] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            Set(b2.hash) ++ process(b2.bundleData)
          case _ => Set[String]()
        }
        depths.reduce(_ ++ _)
      }
      process(bundleData)
    }

    def extractSubBundles: Set[Bundle] = {
      def process(s: Signed[BundleData]): Set[Bundle] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            Set(b2) ++ process(b2.bundleData)
          case _ => Set[Bundle]()
        }
        depths.reduce(_ ++ _)
      }
      process(bundleData)
    }

    /*
        def extractTX: Set[TX] = {
          def process(s: Signed[BundleData]): Set[TX] = {
            val bd = s.data.bundles
            val depths = bd.map {
              case b2: Bundle =>
                process(b2.bundleData)
              case tx: TX => Set(tx)
              case _ => Set[TX]()
            }
            if (depths.nonEmpty) {
              depths.reduce(_ ++ _)
            } else {
              Set()
            }
          }
          process(bundleData)
        }
    */

    // Copy this to temp val on class so as not to re-calculate
    def extractIds: Set[Id] = {
      def process(s: Signed[BundleData]): Set[Id] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            b2.bundleData.encodedPublicKeys.headOption.map{Id}.toSet ++ process(b2.bundleData)
          case _ => Set[Id]()
        }
        depths.reduce(_ ++ _) ++ s.encodedPublicKeys.headOption.map{Id}.toSet
      }
      process(bundleData)
    }

    def maxStackDepth: Int = {
      def process(s: Signed[BundleData]): Int = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            process(b2.bundleData) + 1
          case _ => 0
        }
        depths.max
      }
      process(bundleData) + 1
    }

    def totalNumEvents: Int = {
      def process(s: Signed[BundleData]): Int = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            process(b2.bundleData) + 1
          case _ => 1
        }
        depths.sum
      }
      process(bundleData) + 1
    }

    def roundHash: String = {
      bundleNumber.toString
    }

  }

  final case class Gossip[T <: ProductHash](event: Signed[T]) extends ProductHash with RemoteMessage
    with Fiber
    with GossipMessage {
    def iter: Seq[Signed[_ >: T <: ProductHash]] = {
      def process[Q <: ProductHash](s: Signed[Q]): Seq[Signed[ProductHash]] = {
        s.data match {
          case Gossip(g) =>
            val res = process(g)
            res :+ g
          case _ => Seq()
        }
      }
      process(event) :+ event
    }
    def stackDepth: Int = {
      def process[Q <: ProductHash](s: Signed[Q]): Int = {
        s.data match {
          case Gossip(g) =>
            process(g) + 1
          case _ => 0
        }
      }
      process(event) + 1
    }

  }

  // TODO: Move other messages here.
  sealed trait InternalCommand

  final case object GetPeersID extends InternalCommand
  final case object GetPeersData extends InternalCommand
  final case object GetUTXO extends InternalCommand
  final case object GetValidTX extends InternalCommand
  final case object GetMemPoolUTXO extends InternalCommand
  final case object ToggleHeartbeat extends InternalCommand
  final case object InternalHeartbeat extends InternalCommand
  final case object InternalBundleHeartbeat extends InternalCommand

  final case class ValidateTransaction(tx: Transaction) extends InternalCommand

  trait DownloadMessage

  case class DownloadRequest(time: Long = System.currentTimeMillis()) extends DownloadMessage with RemoteMessage
  case class DownloadResponse(
                               maxBundle: Bundle,
                               genesisBundle: Bundle,
                               genesisTX: Transaction
                             ) extends DownloadMessage with RemoteMessage

  final case class SyncData(validTX: Set[Transaction], memPoolTX: Set[Transaction]) extends GossipMessage with RemoteMessage

  case class MissingTXProof(tx: Transaction, gossip: Seq[Gossip[ProductHash]]) extends GossipMessage with RemoteMessage

  final case class RequestTXProof(txHash: String) extends GossipMessage with RemoteMessage

  case class MetricsResult(metrics: Map[String, String])

  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(encodedId: EncodedPublicKey) {
    def short: String = id.toString.slice(15, 20)
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")
    def address: AddressMetaData = pubKeyToAddress(id)
    def b58: String = Base58.encode(id.getEncoded)
    def id: PublicKey = encodedId.toPublicKey
  }

  case class GetId()

  case class GetBalance(account: PublicKey)

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

  final case class VoteCandidate(tx: Transaction, gossip: Seq[Gossip[ProductHash]])

  final case class VoteDataSimpler(accept: Seq[VoteCandidate], reject: Seq[VoteCandidate]) extends ProductHash

  final case class Vote(vote: Signed[VoteData]) extends ProductHash with Fiber

  case class TransactionSerialized(hash: String, sender: String, receiver: String, amount: Long, signers: Set[String], time: Long)
  case class Node(address: String, host: String, port: Int)

}
