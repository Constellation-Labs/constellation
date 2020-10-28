package org.constellation.gossip

import org.constellation.schema.Id

trait GossipError extends Throwable {}

case class IncorrectNextNodeOnPath(incorrect: Option[Id], correct: Option[Id]) extends GossipError

case object EndOfPath extends GossipError
