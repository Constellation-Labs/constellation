package org.constellation.blockchain

import java.util.Date

import com.roundeights.hasher.Implicits._
import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.collection.generic.SeqFactory
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class Block(index: Int, previousHash: String, timestamp: Long, data: String, hash: String, id1: Option[String] = None, recipient: Option[String] = None, d2: Option[String] = None)

object GenesisBlock extends Block(0, "0", 1497359352, "Genesis block", "ccce7d8349cf9f5d9a9c8f9293756f584d02dfdb953361c5ee36809aa0f560b4")

object Chain {
  def apply(id: String): Chain = new Chain(id, Seq(GenesisBlock))

  def apply(id: String, blocks: Seq[Block]): Try[Chain] = {
    if ( validChain(blocks) ) Success(new Chain(id, blocks))
    else Failure(new IllegalArgumentException("Invalid chain specified."))
  }

  @tailrec
  def validChain( chain: Seq[Block] ): Boolean = chain match {
    case singleBlock :: Nil if singleBlock == GenesisBlock => true
    case head :: beforeHead :: tail if validBlock(head, beforeHead) => validChain(beforeHead :: tail)
    case _ => false
  }

  def validBlock(newBlock: Block, previousBlock: Block) =
    previousBlock.index + 1 == newBlock.index &&
    previousBlock.hash == newBlock.previousHash &&
    calculateHashForBlock(newBlock) == newBlock.hash

  def calculateHashForBlock( block: Block ) = calculateHash(block.index, block.previousHash, block.timestamp, block.data)

  def calculateHash(index: Int, previousHash: String, timestamp: Long, data: String) =
    s"$index:$previousHash:$timestamp:$data".sha256.hex
}

case class Chain private(id: String, blocks: Seq[Block] ) {

  import Chain._

  val logger = Logger("BlockChain")

  def addBlock( data: String ): Chain = new Chain(id, generateNextBlock(data) +: blocks)

  def addBlock( block: Block ): Try[Chain] =
    if ( validBlock(block) ) Success(new Chain(id, block +: blocks ))
    else Failure( new IllegalArgumentException("Invalid block added"))

  def firstBlock: Block = blocks.last
  def latestBlock: Block = blocks.head

  def generateNextBlock( blockData: String ): Block = {
    val List(sender, recipient, signature) = blockData.split(":").toList match {
      case payload :: sentBy :: recip :: sig :: Nil => Some(sentBy) :: Some(recip) :: Some(sig) :: Nil
      case payload :: recip :: Nil => Some(id) :: Some(recip) :: None :: Nil

      case payload :: Nil => None :: None :: None :: Nil
      case _ => None :: None :: None :: Nil
    }
    val previousBlock = latestBlock
    val nextIndex = previousBlock.index + 1
    val nextTimestamp = new Date().getTime() / 1000
    val nextHash: String = calculateHash(nextIndex, previousBlock.hash, nextTimestamp, blockData.split(":").headOption.getOrElse("XXX"))
    Block(nextIndex, previousBlock.hash, nextTimestamp, blockData, nextHash, sender, recipient, signature)
  }

  def validBlock( newBlock: Block ): Boolean = Chain.validBlock(newBlock, latestBlock)

}




