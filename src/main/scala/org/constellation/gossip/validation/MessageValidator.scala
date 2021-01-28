package org.constellation.gossip.validation

import cats.data.Validated
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.gossip.state.GossipMessage
import org.constellation.schema.Id
import org.constellation.schema.signature.HashSignature

import scala.annotation.tailrec

class MessageValidator(selfId: Id) extends StrictLogging {

  def validateForForward[A](
    message: GossipMessage[A],
    senderId: Id
  ): Validated[MessageValidationError, GossipMessage[A]] =
    couldSend(message, senderId)
      .andThen(couldReceive)
      .andThen(pathStartsAndEndsInOrigin)
      .andThen(isEndOfCycle)
      .andThen(signatureChainValid)

  private def signatureChainValid[A](
    message: GossipMessage[A]
  ): Validated[MessageValidationError, GossipMessage[A]] = {
    @tailrec
    def validate(message: GossipMessage[A], result: Boolean = true): Boolean =
      popSignature(message) match {
        case Some((previousMessage, signature)) =>
          validate(
            previousMessage,
            result && previousMessage.path.isCurrent(signature.id) && signature.valid(previousMessage.hash)
          )
        case None => result
      }

    if (validate(message)) message.valid else IncorrectSignatureChain(message.path, message.signatures).invalid
  }

  private def pathStartsAndEndsInOrigin[A](
    message: GossipMessage[A]
  ): Validated[MessageValidationError, GossipMessage[A]] =
    if (message.path.isFirst(message.origin) && message.path.isLast(message.origin)) message.valid
    else PathDoesNotStartAndEndWithOrigin.invalid

  private def couldSend[A](
    message: GossipMessage[A],
    senderId: Id
  ): Validated[MessageValidationError, GossipMessage[A]] =
    if (message.path.isPrev(senderId)) message.valid else IncorrectSenderId(senderId).invalid

  private def couldReceive[A](message: GossipMessage[A]): Validated[MessageValidationError, GossipMessage[A]] =
    if (message.path.isCurrent(selfId)) message.valid
    else IncorrectReceiverId(selfId.some, message.path.curr).invalid

  private def isEndOfCycle[A](message: GossipMessage[A]): Validated[MessageValidationError, GossipMessage[A]] =
    if (message.path.isFirst(selfId) && message.path.isLast(selfId)) EndOfCycle.invalid else message.valid

  private def popSignature[A](message: GossipMessage[A]): Option[(GossipMessage[A], HashSignature)] =
    message.signatures match {
      case signatures @ _ +: _ =>
        val previousMessage = message.copy(
          path = message.path.backward,
          signatures = signatures.init
        )
        Some(previousMessage, signatures.last)
      case _ => None
    }
}

object MessageValidator {
  def apply(selfId: Id): MessageValidator = new MessageValidator(selfId)
}
