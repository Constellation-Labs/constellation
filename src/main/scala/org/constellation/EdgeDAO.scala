package org.constellation

import java.util.concurrent.Semaphore

import com.typesafe.scalalogging.StrictLogging
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.{ChannelMessage, ChannelSendRequest, Height, Id}

import scala.collection.concurrent.TrieMap

class ThreadSafeMessageMemPool() extends StrictLogging {

  private var messages = Seq[Seq[ChannelMessage]]()

  val activeChannels: TrieMap[String, Semaphore] = TrieMap()

  val selfChannelNameToGenesisMessage: TrieMap[String, ChannelMessage] = TrieMap()
  val selfChannelIdToName: TrieMap[String, String] = TrieMap()

  val messageHashToSendRequest: TrieMap[String, ChannelSendRequest] = TrieMap()

  def release(messages: Seq[ChannelMessage]): Unit =
    messages.foreach { m =>
      activeChannels.get(m.signedMessageData.data.channelId).foreach {
        _.release()
      }

    }

  // TODO: Fix
  def pull(minCount: Int = 1): Option[Seq[ChannelMessage]] = this.synchronized {
    /*if (messages.size >= minCount) {
      val (left, right) = messages.splitAt(minCount)
      messages = right
      Some(left.flatten)
    } else None*/
    val flat = messages.flatten
    messages = Seq()
    if (flat.isEmpty) None
    else {
      logger.debug(s"Pulled messages from mempool: ${flat.map {
        _.signedMessageData.hash
      }}")
      Some(flat)
    }
  }

  def put(message: Seq[ChannelMessage], overrideLimit: Boolean = false)(implicit dao: DAO): Boolean =
    this.synchronized {
      val notContained = !messages.contains(message)

      if (notContained) {
        if (overrideLimit) {
          // Prepend in front to process user TX first before random ones
          messages = Seq(message) ++ messages

        } else if (messages.size < dao.processingConfig.maxMemPoolSize) {
          messages :+= message
        }
      }
      notContained
    }

  def unsafeCount: Int = messages.size

}

case class PendingDownloadException(id: Id)
    extends Exception(s"Node [${id.short}] is not ready to accept blocks from others.")

// TODO: wkoszycki this one is temporary till (#412 Flatten checkpointBlock in CheckpointCache) is finished
case object MissingCheckpointBlockException extends Exception("CheckpointBlock object is empty.")

case class MissingHeightException(baseHash: String, soeHash: String)
    extends Exception(s"CheckpointBlock ${baseHash} height is missing for soeHash ${soeHash}.")

object MissingHeightException {
  def apply(cb: CheckpointBlock): MissingHeightException = new MissingHeightException(cb.soeHash, cb.soeHash)
}

case class HeightBelow(baseHash: String, height: Long)
    extends Exception(s"CheckpointBlock hash=${baseHash} height=${height} is below the last snapshot height")

object HeightBelow {
  def apply(cb: CheckpointBlock, height: Height): HeightBelow = new HeightBelow(cb.soeHash, height.min)
}

case class PendingAcceptance(baseHash: String)
    extends Exception(s"CheckpointBlock: $baseHash is already pending acceptance phase.")

object PendingAcceptance {
  def apply(cb: CheckpointBlock): PendingAcceptance = new PendingAcceptance(cb.soeHash)
}

case class CheckpointAcceptBlockAlreadyStored(baseHash: String)
    extends Exception(s"CheckpointBlock: ${baseHash} is already stored.")

object CheckpointAcceptBlockAlreadyStored {

  def apply(cb: CheckpointBlock): CheckpointAcceptBlockAlreadyStored =
    new CheckpointAcceptBlockAlreadyStored(cb.soeHash)
}

case class MissingTransactionReference(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock hash=${cb.soeHash} have missing transaction reference.")

object MissingTransactionReference {
  def apply(cb: CheckpointBlock): MissingTransactionReference = new MissingTransactionReference(cb)
}

case class MissingParents(cb: CheckpointBlock)
    extends Exception(s"CheckpointBlock hash=${cb.soeHash} have missing parents.")

object MissingParents {
  def apply(cb: CheckpointBlock): MissingParents = new MissingParents(cb)
}

case class ContainsInvalidTransactionsException(cbHash: String, txsToExclude: List[String])
    extends Exception(s"CheckpointBlock hash=$cbHash contains invalid transactions=$txsToExclude")

object ContainsInvalidTransactionsException {

  def apply(cb: CheckpointBlock, txsToExclude: List[String]): ContainsInvalidTransactionsException =
    new ContainsInvalidTransactionsException(cb.soeHash, txsToExclude)
}
