package org.constellation.primitives


import java.net.InetSocketAddress
import java.security.PublicKey

import constellation.pubKeyToAddress
import org.constellation.crypto.Base58
import org.constellation.util.EncodedPublicKey
import org.constellation.util.{ProductHash, Signed}

import scala.collection.concurrent.TrieMap

// This can't be a trait due to serialization issues
object Schema {

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


  sealed trait ConfigUpdate

  final case class ReputationUpdates(updates: Seq[UpdateReputation]) extends ConfigUpdate

  case class UpdateReputation(id: Id, secretReputation: Option[Double], publicReputation: Option[Double])

  // I.e. equivalent to number of sat per btc
  val NormalizationFactor: Long = 1e8.toLong

  case class SendToAddress(
                            address: Address,
                            amount: Long,
                            account: Option[PublicKey] = None,
                            normalized: Boolean = true,
                            oneTimeUse: Boolean = false,
                            useNodeKey: Boolean = true
                          ) {
    def amountActual: Long = if (normalized) amount * NormalizationFactor else amount
  }

  // TODO: We also need a hash pointer to represent the post-tx counter party signing data, add later
  // TX should still be accepted even if metadata is incorrect, it just serves to help validation rounds.
  case class Address(
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

  case class CounterPartyTXRequest(
                                    dst: Address,
                                    counterParty: Address,
                                    counterPartyAccount: Option[EncodedPublicKey]
                                  ) extends ProductHash

  sealed trait Fiber

  case class TXData(
                     src: Seq[Address],
                     dst: Address,
                     amount: Long,
                     remainder: Option[Address] = None,
                     srcAccount: Option[EncodedPublicKey] = None,
                     dstAccount: Option[EncodedPublicKey] = None,
                     counterPartySigningRequest: Option[Signed[CounterPartyTXRequest]] = None,
                     // ^ this is for extra security where a receiver can confirm a transaction without
                     // revealing it's address key -- requires receiver link dst to counterParty addresses.
                     keyMap: Seq[Int] = Seq(), // for multi-signature transactions
                     confirmationWindowSeconds: Int = 5,
                     genesisTXHash: Option[String] = None,
                     isGenesis: Boolean = false
                     // TODO: Add threshold maps
                   ) extends ProductHash {
    def inverseAmount: Long = -1*amount
  }

  case class TX(tx: Signed[TXData]) extends ProductHash with Fiber {
    def valid: Boolean = {

      // Last key is used for srcAccount when filled out.
      val addressKeys = if (tx.data.srcAccount.nonEmpty) {
        tx.encodedPublicKeys.slice(0, tx.encodedPublicKeys.size - 1)
      } else tx.encodedPublicKeys

      val km = tx.data.keyMap
      val signatureAddresses = if (km.nonEmpty) {
        val store = Array.fill(km.toSet.size)(Seq[PublicKey]())
        km.zipWithIndex.foreach{ case (keyGroupIdx, keyIdx) =>
          store(keyGroupIdx) = store(keyGroupIdx) :+ addressKeys(keyIdx).toPublicKey
        }
        store.map{constellation.pubKeysToAddress}.toSeq
      } else {
        addressKeys.map{_.toPublicKey}.map{constellation.pubKeyToAddress}
      }

      val matchingAccountValid = tx.data.srcAccount.forall(_ == tx.encodedPublicKeys.last)

      val validInputAddresses = signatureAddresses.map{_.address} == tx.data.src.map{_.address}
      validInputAddresses && tx.valid && matchingAccountValid
    }
    def normalizedAmount: Double = tx.data.amount / NormalizationFactor
    def output(outputHash: String): Option[Address] = {
      val d = tx.data
      if (d.dst.address == outputHash) Some(d.dst)
      else if (d.remainder.exists(_.address == outputHash)) d.remainder
      else None
    }
    def pretty: String = s"TX: ${this.short} FROM ${tx.data.src.head.address.slice(0, 5)}" +
      s" TO ${tx.data.dst.address.slice(0, 5)} REMAINDER " +
      s"${tx.data.remainder.map{_.address}.getOrElse("empty").slice(0, 5)} " +
      s"amount ${tx.data.amount}"

    def utxoValid(utxo: TrieMap[String, Long]): Boolean = {
      if (tx.data.isGenesis) !utxo.contains(tx.data.dst.address) else {
        val srcSum = tx.data.src.map {_.address}.flatMap {utxo.get}.sum
        srcSum >= tx.data.amount && valid
      }
    }

    def updateUTXO(UTXO: TrieMap[String, Long]): Unit = {
      val txDat = tx.data
      if (txDat.isGenesis) {
        // UTXO(txDat.src.head.address) = txDat.inverseAmount
        // Move elsewhere ^ too complex.
        UTXO(txDat.dst.address) = txDat.amount
      } else {

        val total = txDat.src.flatMap{s => UTXO.get(s.address)}.sum
        val remainder = total - txDat.amount

        // Empty src balance.
        txDat.src.foreach{
          s =>
            UTXO(s.address) = 0L
        }

        val prevDstBalance = UTXO.getOrElse(txDat.dst.address, 0L)
        UTXO(txDat.dst.address) = prevDstBalance + txDat.amount

        txDat.remainder.foreach{ r =>
          val prv = UTXO.getOrElse(r.address, 0L)
          UTXO(r.address) = prv + remainder
        }

        // Repopulate head src with remainder if no remainder address specified
        if (txDat.remainder.isEmpty && remainder != 0) {
          UTXO(txDat.src.head.address) = remainder
        }

      }
    }
  }

  sealed trait GossipMessage

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

  // Participants are notarized via signatures.
  final case class BundleBlock(
                                parentHash: String,
                                height: Long,
                                txHash: Seq[String]
                              ) extends ProductHash with Fiber

  final case class BundleHash(hash: String) extends Fiber

  final case class BundleData(bundles: Seq[Fiber]) extends ProductHash

  final case class Bundle(
                           bundleData: Signed[BundleData]
                         ) extends ProductHash with Fiber with GossipMessage {


    def extractTX: Set[TX] = {
      def process(s: Signed[BundleData]): Set[TX] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            process(b2.bundleData)
          case tx: TX => Set(tx)
          case _ => Set[TX]()
        }
        depths.reduce(_ ++ _)
      }
      process(bundleData)
    }

    def extractIds: Set[Id] = {
      def process(s: Signed[BundleData]): Set[Id] = {
        val bd = s.data.bundles
        val depths = bd.map {
          case b2: Bundle =>
            b2.bundleData.publicKeys.map{Id}.toSet ++ process(b2.bundleData)
          case _ => Set[Id]()
        }
        depths.reduce(_ ++ _)
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

  }


  final case class Gossip[T <: ProductHash](event: Signed[T]) extends ProductHash
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

  case class DownloadRequest()

  case class DownloadResponse(validTX: Set[TX], validUTXO: Map[String, Long])

  case class SyncData(validTX: Set[TX], memPoolTX: Set[TX])

  case class MissingTXProof(tx: TX, gossip: Seq[Gossip[ProductHash]])

  case class RequestTXProof(txHash: String)

  case class Metrics(metrics: Map[String, String])

  final case class AddPeerFromLocal(address: InetSocketAddress) extends InternalCommand


  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(id: PublicKey) {
    def short: String = id.toString.slice(15, 20)
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")
    def address: Address = pubKeyToAddress(id)
    def b58 = Base58.encode(id.getEncoded)
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
  case class HandShakeMessage(handShake: Signed[HandShake])
  case class HandShakeResponseMessage(handShakeResponse: Signed[HandShakeResponse])

  case class HandShakeResponse(
                                //                   original: Signed[HandShake],
                                response: HandShake,
                                detectedRemote: InetSocketAddress
                              ) extends ProductHash

  case class Peer(
                   id: Id,
                   externalAddress: InetSocketAddress,
                   remotes: Set[InetSocketAddress] = Set(),
                   apiAddress: InetSocketAddress = null
                 ) extends ProductHash


}
