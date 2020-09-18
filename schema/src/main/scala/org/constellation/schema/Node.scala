package org.constellation.schema

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class Node(address: String, host: String, port: Int)

object Node {
  implicit val nodeEncoder: Encoder[Node] = deriveEncoder
  implicit val nodeDecoder: Decoder[Node] = deriveDecoder
}
