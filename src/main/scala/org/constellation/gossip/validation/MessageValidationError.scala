package org.constellation.gossip.validation

import org.constellation.schema.Id

trait MessageValidationError extends Throwable {}

case class IncorrectReceiverId(incorrect: Option[Id], correct: Option[Id]) extends MessageValidationError

case object EndOfCycle extends MessageValidationError

case class IncorrectSenderId(sender: Id) extends MessageValidationError

case object IncorrectSignatureChain extends MessageValidationError

case object PathDoesNotStartAndEndWithOrigin extends MessageValidationError
