package org.constellation.schema.edge

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.signature.Signable

/**
  * Basic edge format for linking two hashes with an optional piece of data attached. Similar to GraphX format.
  * Left is topologically ordered before right
  *
  * @param parents : HyperEdge parent references
  * @param data    : Optional hash reference to attached information
  */
case class ObservationEdge( // TODO: Consider renaming to ObservationHyperEdge or leave as is?
  parents: Seq[TypedEdgeHash],
  data: TypedEdgeHash
) extends Signable {
  override def getEncoding = {
    val numParents = parents.length //note, we should not use magick number 2 here, state channels can have multiple
    val encodedParentHashRefs = runLengthEncoding(parents.map(_.hashReference): _*)
    numParents + encodedParentHashRefs + data.hashReference
  }
}

object ObservationEdge {
  implicit val observationEdgeEncoder: Encoder[ObservationEdge] = deriveEncoder
  implicit val observationEdgeDecoder: Decoder[ObservationEdge] = deriveDecoder
}
