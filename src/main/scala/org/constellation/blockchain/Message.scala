package org.constellation.blockchain

import java.security.{PrivateKey, PublicKey}

import akka.actor.ActorRef

import scala.collection.mutable
import constellation._
import org.constellation.wallet.KeyUtils

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
  def hash: String
  val senderPubKey: PublicKey
  val counterPartyPubKey: PublicKey
  val amount: Long
}


case class Transaction(
                        sequenceNum: Long,
                        senderPubKey: PublicKey,
                        counterPartyPubKey: PublicKey,
                        amount: Long,
                        senderSignature: String = "",
                        counterPartySignature: String = "",
                        time: Long = System.currentTimeMillis()
                      ) extends Tx {


  private def senderSignInput: Array[Byte] =
    Seq(sequenceNum, senderPubKey, counterPartyPubKey, amount).json.getBytes()

  def senderSign(privateKey: PrivateKey): Transaction = {
    this.copy(
      senderSignature = KeyUtils.base64(KeyUtils.signData(
        senderSignInput
      )(privateKey))
    )
  }

  private def counterPartySignInput =
    Seq(sequenceNum, senderPubKey, counterPartyPubKey, amount, senderSignature).json.getBytes()

  def counterPartySign(privateKey: PrivateKey): Transaction = this.copy(
    counterPartySignature = KeyUtils.base64(KeyUtils.signData(
      counterPartySignInput
    )(privateKey))
  )

  // Hash of TX info
  def hash: String = Seq(
    sequenceNum, senderPubKey, counterPartyPubKey, amount, senderSignature, counterPartySignature
  ).json.sha256

  def validCounterPartySignature: Boolean = {
    KeyUtils.verifySignature(
      counterPartySignInput, KeyUtils.fromBase64(counterPartySignature)
    )(counterPartyPubKey)
  }

  def validSenderSignature: Boolean = {
    KeyUtils.verifySignature(
      senderSignInput, KeyUtils.fromBase64(senderSignature)
    )(senderPubKey)
  }

  def valid: Boolean = validSenderSignature && validCounterPartySignature
}


// Using this case class below may be more typesafe / helpful later.
// For now we're just going to use a single transaction.
/*
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
*/

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
  * @param parentHash the hash pointer to the previous block.
  * @param height the sequence number, current block number.
  * @param consensusResultHashPointer consensus result, hashed
  * @param round the current round of consensus.
  * @param signature the signature created using node's secret key on the concatenation of the binary representation of the five items above.
  */
case class Block(parentHash: String,
                 height: Long,
                 signature: String = "",
                 consensusResultHashPointer: mutable.HashMap[ActorRef, Option[BlockData]] = mutable.HashMap(), //TODO replace with compressed hash of each node's decision on consensus, not proposed subset as shown here (for stubbing byzantine consensus) which will also get added to the txBuffer
                 round: Long = 0L,
                 transactions: Seq[Transaction] = Seq()
                ) extends BlockData {

  // TODO : Use the rest of the info here. This is just for getting an MVP
  def hash: String = (Seq(height, parentHash) ++ transactions.map{_.hash}).json.sha256

}
