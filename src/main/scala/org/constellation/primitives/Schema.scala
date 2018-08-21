package org.constellation.primitives


import java.net.InetSocketAddress
import java.security.PublicKey

import akka.actor.ActorRef
import cats.kernel.Monoid
import constellation.pubKeyToAddress
import org.constellation.LevelDB.DBPut
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.crypto.Base58
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.util._

import scala.collection.{SortedSet, mutable}
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
                                       tx: Option[Transaction],
                                       observed: Boolean,
                                       inMemPool: Boolean,
                                       confirmed: Boolean,
                                       numGossipChains: Int,
                                       gossipStackDepths: Seq[Int],
                                       gossip: Seq[Gossip[ProductHash]]
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
  case class Transaction(
                          txData: Signed[TransactionData]
                        ) extends Fiber with GossipMessage with ProductHash with RemoteMessage {
    def valid: Boolean = txData.valid
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

  object EdgeHashType extends Enumeration {
    type EdgeHashType = Value
    val AddressHash, CheckpointHash, TransactionDataHash, ValidationHash, TransactionHash = Value
  }

  case class TransactionEdgeData(amount: Long, salt: Long = Random.nextLong()) extends ProductHash
  case class CheckpointEdgeData(hashes: Seq[String]) extends ProductHash
  case class TypedEdgeHash(hash: String, hashType: EdgeHashType)
  case class ObservationEdge(left: TypedEdgeHash, right: TypedEdgeHash, data: Option[TypedEdgeHash] = None) extends ProductHash


  case class ResolvedObservationEdge[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (left: L, right: R, data: Option[D] = None)

  // Order is like: TransactionData -> TX -> CheckpointBlock -> ObservationEdge -> SignedObservationEdge

  case class TX(signatureBatch: SignatureBatch) extends ProductHash

  case class CheckpointBlock(transactions: Set[String]) extends ProductHash

  case class SignedObservationEdge(signatureBatch: SignatureBatch) extends ProductHash

  case class EdgeCell(members: mutable.SortedSet[EdgeSheaf])

  case class ResolvedTX(edge: ResolvedEdgeData[Address, Address, TransactionEdgeData]) {

    def src: Address = edge.resolvedObservationEdge.left
    def dst: Address = edge.resolvedObservationEdge.right

    // TODO: Add proper exception on empty option
    def amount : Long = edge.resolvedObservationEdge.data.get.amount

  }

  case class ResolvedCB(edge: ResolvedEdgeData[SignedObservationEdge, CheckpointEdgeData, Nothing])

  case class ResolvedEdgeData[L <: ProductHash, R <: ProductHash, +D <: ProductHash]
  (
    observationEdge: ObservationEdge,
    signedObservationEdge: SignedObservationEdge,
    resolvedObservationEdge: ResolvedObservationEdge[L,R,D]
  ) {

    def store(db: ActorRef): Unit = {
      db ! DBPut(signedObservationEdge.signatureBatch.hash, observationEdge)
      db ! DBPut(signedObservationEdge.hash, signedObservationEdge)
      resolvedObservationEdge.data.foreach {
        ted =>
          db ! DBPut(ted.hash, ted)
      }
    }

  }

  case class Address(address: String) extends ProductHash {
    override def hash: String = address
  }

  case class EdgeSheaf(
                        signedObservationEdge: SignedObservationEdge,
                        parent: String,
                        height: Long,
                        depth: Int,
                        score: Double
                      )

  case class AddressCacheData(balance: Long, reputation: Option[Double] = None)


  case class ResolvedCBObservation(
                                    resolvedTX: Seq[ResolvedTX],
                                    resolvedCB: ResolvedCB
                                  ) {
    def store(db: ActorRef): Unit = {
      resolvedTX.foreach { rt =>
        rt.edge.store(db)
      }
      resolvedCB.edge.store(db)
    }
  }

  case class PeerIPData(canonicalHostName: String, port: Option[Int])

  case class GenesisObservation(
                                 genesis: ResolvedCBObservation,
                                 initialDistribution: ResolvedCBObservation
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

  case class Metrics(metrics: Map[String, String])

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
