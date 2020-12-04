package org.constellation.schema

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class NodeStateInfo(
  nodeState: NodeState,
  addresses: Seq[String],
  nodeType: NodeType
)

object NodeStateInfo {
  implicit val nodeStateInfoEncoder: Encoder[NodeStateInfo] = deriveEncoder
  implicit val nodeStateInfoDecoder: Decoder[NodeStateInfo] = deriveDecoder
}
