package org.constellation.gossip

import org.constellation.schema.Id

sealed trait GossipError extends Throwable {}

case class MissingClientForId(id: Id) extends GossipError {
  override def getMessage: String = s"Couldn't find HTTP client for Id $id"
}
