package org.constellation.schema.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.signature.Signed

case class SnapshotProposalPayload(
  proposal: Signed[SnapshotProposal],
  filterData: FilterData
)

object SnapshotProposalPayload {
  implicit val encoder: Encoder[SnapshotProposalPayload] = deriveEncoder
  implicit val decoder: Decoder[SnapshotProposalPayload] = deriveDecoder
}
