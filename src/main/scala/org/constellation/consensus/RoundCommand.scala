package org.constellation.consensus
import org.constellation.primitives.{CheckpointBlock, Schema}

case class FacilitatorId(id: Schema.Id) extends AnyVal
case class RoundId(id: String) extends AnyVal

// TODO

sealed trait RoundCommand {
  def roundId: RoundId
}

case class NotifyFacilitators(roundId: RoundId) extends RoundCommand
case class BlockCreationRoundStarted(roundId: RoundId) extends RoundCommand
case class BroadcastProposal(roundId: RoundId, cb: CheckpointBlock) extends RoundCommand
case class ReceivedProposal(roundId: RoundId, facilitatorId: FacilitatorId, cb: CheckpointBlock) extends RoundCommand
case class UnionProposals(roundId: RoundId) extends RoundCommand
case class BroadcastMajorityUnionedBlock(roundId: RoundId, cb: CheckpointBlock) extends RoundCommand
case class ReceivedMajorityUnionedBlock(roundId: RoundId, cb: CheckpointBlock) extends RoundCommand
