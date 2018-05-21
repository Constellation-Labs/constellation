package org.constellation.primitives


import java.security.PublicKey

import akka.stream.scaladsl.Balance
import org.constellation.util.{ProductHash, Signed}

import scala.collection.mutable

// This can't be a trait due to serialization issues
object Schema {

  // I.e. equivalent to number of sat per btc
  val NormalizationFactor: Long = 1e8.toLong


  case class SendToAddress(
                            address: Address,
                            amount: Long,
                            account: Option[PublicKey] = None,
                            normalized: Boolean = true,
                            oneTimeUse: Boolean = false
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
                                    counterPartyAccount: Option[PublicKey]
                                  ) extends ProductHash

  sealed trait Event

  case class TXData(
                     src: Seq[Address],
                     dst: Address,
                     amount: Long,
                     remainder: Option[Address] = None,
                     srcAccount: Option[PublicKey] = None,
                     dstAccount: Option[PublicKey] = None,
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

  case class TX(tx: Signed[TXData]) extends ProductHash with Event {
    def valid: Boolean = {

      // Last key is used for srcAccount when filled out.
      val addressKeys = if (tx.data.srcAccount.nonEmpty) {
        tx.publicKeys.slice(0, tx.publicKeys.size - 1)
      } else tx.publicKeys

      val km = tx.data.keyMap
      val signatureAddresses = if (km.nonEmpty) {
        val store = Array.fill(km.toSet.size)(Seq[PublicKey]())
        km.zipWithIndex.foreach{ case (keyGroupIdx, keyIdx) =>
          store(keyGroupIdx) = store(keyGroupIdx) :+ addressKeys(keyIdx)
        }
        store.map{constellation.pubKeysToAddress}.toSeq
      } else {
        addressKeys.map{constellation.pubKeyToAddress}
      }

      val matchingAccountValid = tx.data.srcAccount.forall(_ == tx.publicKeys.last)

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

    def utxoValid(utxo: mutable.HashMap[String, Long]): Boolean = {
      val srcSum = tx.data.src.map{_.hash}.flatMap{utxo.get}.sum
      srcSum >= tx.data.amount && valid
    }

    def updateUTXO(UTXO: mutable.HashMap[String, Long]): Unit = {
      val txDat = tx.data
      if (txDat.isGenesis) {
        // UTXO(txDat.src.head.address) = txDat.inverseAmount
        // Move elsewhere ^ too complex.
        UTXO(txDat.dst.address) = txDat.amount
      } else {

        val total = txDat.src.map{s => UTXO(s.address)}.sum
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

  final case class VoteData(accept: Seq[TX], reject: Seq[TX]) extends ProductHash

  final case class Vote(vote: Signed[VoteData]) extends ProductHash with Event

  // Participants are notarized via signatures.
  final case class BundleBlock(
                                parentHash: String,
                                height: Long,
                                txHash: Seq[String]
                              ) extends ProductHash with Event

  final case class BundleHash(hash: String) extends Event

  final case class BundleData(bundles: Seq[Event]) extends ProductHash

  final case class Bundle(
                           bundleData: Signed[BundleData]
                         ) extends ProductHash with Event {

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

  case class Gossip[T <: ProductHash](event: Signed[T]) extends ProductHash with Event {
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
  final case object ToggleHeartbeat extends InternalCommand

  final case class ValidateTransaction(tx: TX) extends InternalCommand


}
