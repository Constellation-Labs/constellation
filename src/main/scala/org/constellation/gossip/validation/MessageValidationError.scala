package org.constellation.gossip.validation

import cats.implicits._
import org.constellation.gossip.sampling.GossipPath
import org.constellation.schema.Id
import org.constellation.schema.signature.HashSignature
import org.constellation.collection.SeqUtils.PathSeqOps

sealed trait MessageValidationError extends Throwable {}

case class IncorrectReceiverId(incorrect: Option[Id], correct: Option[Id]) extends MessageValidationError {
  override def getMessage: String = s"Receiver Id is incorrect in message: $incorrect but should be $correct"
}

case object EndOfCycle extends MessageValidationError {
  override def getMessage: String = s"Message cycle is finished. Can't forward."
}

case class IncorrectSenderId(sender: Id) extends MessageValidationError {
  override def getMessage: String = s"Sender Id is incorrect in message: $sender"
}

case class IncorrectSignatureChain(path: GossipPath, signatures: IndexedSeq[HashSignature])
    extends MessageValidationError {
  override def getMessage: String = {
    val chainIds = signatures.map(_.id).formatPath
    val pathIds = path.toIndexedSeq.formatPath
    s"Signature chain incorrect: Got $chainIds for path $pathIds"
  }
}

case object PathDoesNotStartAndEndWithOrigin extends MessageValidationError {}
