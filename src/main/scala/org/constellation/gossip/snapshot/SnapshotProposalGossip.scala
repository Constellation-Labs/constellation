package org.constellation.gossip.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id

import scala.collection.SortedMap

case class SnapshotProposalGossip(hash: String, height: Long, reputation: SortedMap[Id, Double])

object SnapshotProposalGossip {
  implicit val snapshotProposalCodec: Encoder[SnapshotProposalGossip] = deriveEncoder
  implicit val snapshotProposalDecoder: Decoder[SnapshotProposalGossip] = deriveDecoder
}
