package org.constellation.schema.v2.checkpoint

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.edge.Edge

case class CheckpointEdge(edge: Edge[CheckpointEdgeData]) {

  def plus(other: CheckpointEdge) = this.copy(edge = edge.plus(other.edge))
}

object CheckpointEdge {
  implicit val checkpointEdgeDataEncoder: Encoder[Edge[CheckpointEdgeData]] = deriveEncoder
  implicit val checkpointEdgeDataDecoder: Decoder[Edge[CheckpointEdgeData]] = deriveDecoder

  implicit val checkpointEdgeEncoder: Encoder[CheckpointEdge] = deriveEncoder
  implicit val checkpointEdgeDecoder: Decoder[CheckpointEdge] = deriveDecoder
}
