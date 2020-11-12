package org.constellation.util

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.{NodeState, NodeType}

case class NodeStateInfo(
  nodeState: NodeState,
  addresses: Seq[String] = Seq(),
  nodeType: NodeType = NodeType.Full
) // TODO: Refactor, addresses temp for testing

object NodeStateInfo {
  implicit val nodeStateInfoEncoder: Encoder[NodeStateInfo] = deriveEncoder
  implicit val nodeStateInfoDecoder: Decoder[NodeStateInfo] = deriveDecoder
}
