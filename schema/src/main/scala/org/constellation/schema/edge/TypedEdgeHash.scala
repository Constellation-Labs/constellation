package org.constellation.schema.edge

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

/**
  * Wrapper for encapsulating a typed hash reference
  *
  * @param hashReference : String of hashed value or reference to be signed
  * @param hashType : Strictly typed from set of allowed edge formats
  */ // baseHash Temporary to debug heights missing
case class TypedEdgeHash(
  hashReference: String,
  hashType: EdgeHashType,
  baseHash: Option[String] = None
)

object TypedEdgeHash {
  implicit val typedEdgeHashEncoder: Encoder[TypedEdgeHash] = deriveEncoder
  implicit val typedEdgeHashDecoder: Decoder[TypedEdgeHash] = deriveDecoder
}
