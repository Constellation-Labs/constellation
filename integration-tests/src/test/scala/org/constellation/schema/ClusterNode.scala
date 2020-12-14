package org.constellation.schema

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class ClusterNode(alias: String, id: Id, ip: HostPort, status: NodeState, reputation: Long)

object ClusterNode {
  implicit val clusterNodeEncoder: Encoder[ClusterNode] = deriveEncoder
  implicit val clusterNodeDecoder: Decoder[ClusterNode] = deriveDecoder
}
