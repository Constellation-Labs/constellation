package org.constellation.primitives


import java.net.InetSocketAddress
import java.security.PublicKey

import constellation.pubKeyToAddress
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.crypto.Base58
import org.constellation.util.{EncodedPublicKey, HashSignature, ProductHash, Signed}

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
                                       tx: Option[TX],
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

  case class TXData(
                     src: String,
                     dst: String,
                     amount: Long,
                     time: Long = System.currentTimeMillis()
                   ) extends ProductHash with GossipMessage {
    def inverseAmount: Long = -1*amount
    def normalizedAmount: Long = amount / NormalizationFactor
    def pretty: String = s"TX: $short FROM: ${src.slice(0, 8)} " +
      s"TO: ${dst.slice(0, 8)} " +
      s"AMOUNT: $normalizedAmount"

    def srcLedgerBalance(ledger: TrieMap[String, Long]): Long = {
      ledger.getOrElse(this.src, 0)
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

  case class TX(
                 txData: Signed[TXData]
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

  // TODO: Change options to ResolvedBundleMetaData

  case class BundleMetaData(
                             bundle: Bundle,
                             height: Option[Int] = None,
                             reputations: Map[String, Long] = Map(),
                             totalScore: Option[Double] = None,
                             rxTime: Long = System.currentTimeMillis(),
                             transactionsResolved: Boolean = false
                           ) {
    def isResolved: Boolean = reputations.nonEmpty && transactionsResolved
  }

  final case class PeerSyncHeartbeat(
                                      maxBundle: Bundle,
                                      validLedger: Map[String, Long]
                                    ) extends GossipMessage with RemoteMessage {
    def safeMaxBundle = Option(maxBundle)
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

  final case class ValidateTransaction(tx: TX) extends InternalCommand

  trait DownloadMessage

  case class DownloadRequest(time: Long = System.currentTimeMillis()) extends DownloadMessage with RemoteMessage
  case class DownloadResponse(
                               maxBundle: Bundle,
                               genesisBundle: Bundle,
                               genesisTX: TX
                             ) extends DownloadMessage with RemoteMessage

  final case class SyncData(validTX: Set[TX], memPoolTX: Set[TX]) extends GossipMessage with RemoteMessage

  case class MissingTXProof(tx: TX, gossip: Seq[Gossip[ProductHash]]) extends GossipMessage with RemoteMessage

  final case class RequestTXProof(txHash: String) extends GossipMessage with RemoteMessage

  case class Metrics(metrics: Map[String, String])

  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(encodedId: EncodedPublicKey) {
    def short: String = id.toString.slice(15, 20)
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")
    def address: AddressMetaData = pubKeyToAddress(id)
    def b58 = Base58.encode(id.getEncoded)
    def id = encodedId.toPublicKey
  }

  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake(
                        originPeer: Signed[Peer],
                        requestExternalAddressCheck: Boolean = false
                        //           peers: Seq[Signed[Peer]],
                        //          destination: Option[InetSocketAddress] = None
                      ) extends ProductHash

  // These exist because type erasure messes up pattern matching on Signed[T] such that
  // you need a wrapper case class like this
  case class HandShakeMessage(handShake: Signed[HandShake]) extends RemoteMessage
  case class HandShakeResponseMessage(handShakeResponse: Signed[HandShakeResponse]) extends RemoteMessage

  case class HandShakeResponse(
                                //                   original: Signed[HandShake],
                                response: HandShake,
                                detectedRemote: InetSocketAddress
                              ) extends ProductHash with RemoteMessage

  case class Peer(
                   id: Id,
                   externalAddress: InetSocketAddress,
                   remotes: Set[InetSocketAddress] = Set(),
                   apiAddress: InetSocketAddress = null
                 ) extends ProductHash





  // Experimental below

  case class CounterPartyTXRequest(
                                    dst: AddressMetaData,
                                    counterParty: AddressMetaData,
                                    counterPartyAccount: Option[EncodedPublicKey]
                                  ) extends ProductHash


  final case class ConflictDetectedData(detectedOn: TX, conflicts: Seq[TX]) extends ProductHash

  final case class ConflictDetected(conflict: Signed[ConflictDetectedData]) extends ProductHash with GossipMessage

  final case class VoteData(accept: Seq[TX], reject: Seq[TX]) extends ProductHash {
    // used to determine what voting round we are talking about
    def voteRoundHash: String = {
      accept.++(reject).sortBy(t => t.hashCode()).map(f => f.hash).mkString("-")
    }
  }

  final case class VoteCandidate(tx: TX, gossip: Seq[Gossip[ProductHash]])

  final case class VoteDataSimpler(accept: Seq[VoteCandidate], reject: Seq[VoteCandidate]) extends ProductHash

  final case class Vote(vote: Signed[VoteData]) extends ProductHash with Fiber



  case class TransactionSerialized(hash: String, sender: String, receiver: String, amount: Long, signers: Set[String])
  case class Node(address: String, host: String, port: Int)


}
