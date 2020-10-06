package org.constellation.gossip

import org.constellation.schema.Id

sealed trait GossipError extends Throwable {}

case class IncorrectReceiverId(incorrect: Option[Id], correct: Option[Id]) extends GossipError {
  override def getMessage: String =
    s"Incorrect receiver Id. Message should have been sent to $correct instead of $incorrect."
}

case object EndOfCycle extends GossipError {
  override def getMessage: String = s"Message cycle is finished. Can't forward."
}

case class MissingClientForId(id: Id) extends GossipError {
  override def getMessage: String = s"Couldn't find HTTP client for Id $id"
}

case class NotEnoughPeersForFanout(fanout: Int, peers: Int) extends GossipError {
  override def getMessage: String = s"Not enough peers ($peers) for fanout $fanout"
}

case class IncorrectSenderId(id: Id) extends GossipError {
  override def getMessage: String = s"Sender Id is incorrect in message: $id"
}
