package org.constellation.gossip

import org.constellation.schema.Id

trait GossipError extends Throwable {}

case class IncorrectReceiverId(incorrect: Option[Id], correct: Option[Id]) extends GossipError

case object EndOfCycle extends GossipError

case class MissingClientForId(id: Id) extends GossipError

case object NoneOfNodesFailedOnPath extends GossipError

case class NotEnoughPeersForFanout(fanout: Int, peers: Int) extends GossipError

case class IncorrectSenderId(id: Id) extends GossipError
