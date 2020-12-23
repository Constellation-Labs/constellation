package org.constellation.schema.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signable

import scala.collection.SortedMap

case class SnapshotProposal(override val hash: String, height: Long, reputation: SortedMap[Id, Double]) extends Signable

object SnapshotProposal {
  implicit val smDecoder: Decoder[SortedMap[Id, Double]] =
    Decoder.decodeMap[Id, Double].map(m => SortedMap(m.toSeq: _*))

  implicit val smEncoder: Encoder[SortedMap[Id, Double]] =
    Encoder.encodeMap[Id, Double].contramap(m => m.toMap)

  implicit val snapshotProposalEncoder: Encoder[SnapshotProposal] = deriveEncoder
  implicit val snapshotProposalDecoder: Decoder[SnapshotProposal] = deriveDecoder
}
