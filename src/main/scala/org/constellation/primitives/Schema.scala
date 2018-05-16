package org.constellation.primitives


import java.security.PublicKey

import akka.stream.scaladsl.Balance
import org.constellation.util.{ProductHash, Signed}

// This can't be a trait due to serialization issues
object Schema {

  // I.e. equivalent to number of sat per btc
  val NormalizationFactor: Long = 1e8.toLong

  case class AddressCache(
                         cache: Map[Address, AddressVertexCache]
                         ) {
    def balance(a: Address): Option[Long] = cache.get(a).map{
      _.down.map{_.tx.data.amount}.sum
    }
  }

  case class SendToAddress(
                            address: Address,
                            amount: Long,
                            account: Option[PublicKey],
                            normalized: Boolean = true
                          ) {
    def amountActual: Long = if (normalized) amount * NormalizationFactor else amount
  }

  // TODO: Revisit - need more indices here
  case class AddressVertexCache(
                         up: Option[TX],
                         down: Option[TX] = None // Not present for genesis
                         )

  case class Address(
                      address: String,
                      balance: Long = 0L,
                      lastTransactionHash: Option[String] = None,
                      rootTransactionHash: Option[String] = None,
                      // ^ the hash of the first transaction that created srcAddress
                      // ^ the hash last depositing to srcAddress is stored under src.lastTransactionHash
                      // These will be blank for the DESTINATION address of a new transaction
                      // A second transaction to the same destination updates both quantities
                      lastForwardHash: Option[String] = None,
                      // ^ the data dependency link of a transaction using an address in this chains history.
                      lastDirectChainHash: Option[String] = None,
                      isGenesis: Boolean = false,
                    // ^ TODO: Remove and use genesisTXHash presence.
                      genesisTXHash: Option[String] = None
                    ) extends ProductHash {
    def normalizedBalance: Long = balance / NormalizationFactor
  }

  case class CounterPartyTXRequest(dst: Address, counterParty: Address) extends ProductHash

  /*
  Forwarding rules:

  - History is immutable, once an address is 'spent' the data can no longer be modified.

  - This is designed to attempt to prevent cycles in the DAG (i.e. re-use of a key)
    and ensure key uniqueness

  - The remainder serves as the 'forwarding address' to continue receiving funds / and/or act as a terminal
 for adding additional data under the re-use of a previous address

  - Forwarding addresses are not guaranteed respond quickly due to large number of hashes.

  - If the remainder is missing, the output is used to forward.

  - This is how we continue to support people putting up a QR code on TV for instance or long
  standing addresses while preserving immutability. This is not an ideal use case, as
  the closer we can get to one-time-use addresses the more efficient the data structure becomes.

  If someone doesn't ever send from an address, this isn't an issue at all. It's only once they
  attempt to make a TX outward from the address that this is a problem.

  Receipt addresses must act like a 'terminal', only allowing 1 new TX to be added to their data endpoint
  at a time, this is synonymous with the Ethereum account increment counter - except much better.

  Also - the network should vastly prioritize addresses that only receive a single TX, we want
  to discourage using the same destination address for multiple TXs because it will speed
  up the hashing optimizations dramatically

  We actually can still allow spends from an existing address (although we should incredibly strongly
  discourage this because it messes up the hash lookups) but it still needs to be added to the DAG
  AFTER the forwarding address in order to preserve immutability. Requires more discussion.
   */
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
                 allowForwarding: Boolean = false,
                 allowReuse: Boolean = false,
                 allowMultipleInputTXs: Boolean = false,
                 // ^ More optimization / security options which help keep the DAG clean.
                 isGenesis: Boolean = false, // TODO: Remove this and rely on presence of genesisTXHash
                 // it was just simpler to use this flag for now for other reasons.
                 genesisTXHash: Option[String] = None
               ) extends ProductHash {
    def inverseAmount: Long = -1*amount
    // def remainderAmount
  }

  case class TX(tx: Signed[TXData]) extends ProductHash {
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
  }

  case class Gossip[T <: ProductHash](event: Signed[T]) extends ProductHash {
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


}
