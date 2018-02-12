package org.constellation.blockchain

import akka.actor.ActorRef

import scala.collection.mutable

/**
  * Created by Wyatt on 11/11/17.
  */

/**
  * Parent type for data meant for storage in Blocks
  */
trait BlockData

/**
  * Parent type for data meant for storage in Blocks
  */
trait Tx extends BlockData {
  val id: String
  val senderPubKey: String
  val counterPartyPubKey: String
  val amount: Long
}

/**
  *
  * @param hashPointer the hash pointer to the previous block.
  * @param id the transaction identifier,it should be generated using a cryptographically secure pseudo-random number generator by the initiator of the transaction.
  * @param sequenceNum the sequence number, current block number.
  * @param senderPubKey the public key of this node.
  * @param counterPartyPubKey the public key of the counterparty v.
  * @param amount amount to be sent.
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class Transaction(hashPointer: Array[Byte],
                       id: String,
                       sequenceNum: Long,
                       senderPubKey: String,
                       counterPartyPubKey: String,
                       amount: Long,
                       signature: String) extends Tx

/**
  *
  * @param id the transaction identifier,it should be generated using a cryptographically secure pseudo-random number generator by the initiator of the transaction.
  * @param counterPartyPubKey the public key of the counterparty v.
  * @param amount amount to be sent.
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class SignTransaction(id: String,
                           senderPubKey: String,
                           counterPartyPubKey: String,
                           amount: Long,
                           signature: String) extends Tx

/**
  *
  * @param txs proposed transactions for the block
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class CheckpointMessage(txs: Seq[Transaction],
                             signature: String) extends BlockData

/**
  *
  * @param facilitators nodes selected for round of consensus
  */
case class Facilitators(facilitators: Seq[Transaction])

/**
  *
  * @param hashPointer the hash pointer to the previous block.
  * @param sequenceNum the sequence number, current block number.
  * @param consensusResultHashPointer consensus result, hashed
  * @param round the current round of consensus.
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class CheckpointBlock(hashPointer: String,
                           sequenceNum: Long,
                           signature: String,
                           consensusResultHashPointer: mutable.HashMap[ActorRef, Option[BlockData]] = mutable.HashMap.empty[ActorRef, Option[BlockData]], //TODO replace with compressed hash of each node's decision on consensus, not proposed subset as shown here (for stubbing byzantine consensus) which will also get added to the txBuffer
                           round: Long = 0L) extends BlockData
