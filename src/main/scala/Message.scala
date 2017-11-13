import akka.actor.ActorRef

import scala.collection.mutable

/**
  * Created by Wyatt on 11/11/17.
  */

/**
  * Parent type for data meant for storage in Blocks
  */
trait Block

/**
  *
  * @param hashPointer the hash pointer to the previous block.
  * @param sequenceNum the sequence number, current block number.
  * @param id thetransactionidentifier,itshouldbegeneratedusingacryptographically secure pseudo-random number generator by the initiator of the transaction.
  * @param counterPartyPubKey the public key of the counterparty v.
  * @param message the transaction message.
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class Transaction(hashPointer: Array[Byte],
                       sequenceNum: Long,
                       id: String,
                       counterPartyPubKey: String,
                       message: String,
                       signature: String) extends Block

/**
  *
  * @param txs proposed transactions for the block
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class CheckpointMessage(txs: Seq[Transaction],
                       signature: String) extends Block

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
case class CheckpointBlock(hashPointer: Array[Byte],
                           sequenceNum: Long,
                           consensusResultHashPointer: mutable.HashMap[ActorRef, Option[Block]], //TODO replace with compressed hash of each node's decision on consensus, not proposed subset as shown here (for stubbing byzantine consensus) which will also get added to the txBuffer
                           round: Long,
                           signature: String) extends Block
