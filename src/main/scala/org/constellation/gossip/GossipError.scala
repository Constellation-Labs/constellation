package org.constellation.gossip

import org.constellation.schema.Id

trait GossipError extends Throwable {}

case class MissingClientForId(id: Id) extends GossipError

case object NoneOfNodesFailedOnPath extends GossipError
