package org.constellation.schema.v2

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.v2.checkpoint.CheckpointBlock

case class GenesisObservation(
  genesis: CheckpointBlock,
  initialDistribution: CheckpointBlock,
  initialDistribution2: CheckpointBlock
) {

  def notGenesisTips(tips: Seq[CheckpointBlock]): Boolean =
    !tips.contains(initialDistribution) && !tips.contains(initialDistribution2)

}

object GenesisObservation {
  implicit val genesisEncoder: Encoder[GenesisObservation] = deriveEncoder
  implicit val genesisDecoder: Decoder[GenesisObservation] = deriveDecoder
}
