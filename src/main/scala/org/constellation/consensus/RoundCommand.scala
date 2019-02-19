package org.constellation.consensus

case class RoundId(id: String) extends AnyVal

// TODO
case class CheckpointBlockProposal()
case class MajorityUnionedBlock()

sealed trait RoundCommand {
  def roundId: RoundId
}

case class NotifyFacilitators(roundId: RoundId) extends RoundCommand
case class BlockCreationRoundStarted(roundId: RoundId) extends RoundCommand
case class SendProposal(roundId: RoundId, cb: CheckpointBlockProposal) extends RoundCommand
case class ReceivedProposal(roundId: RoundId, cb: CheckpointBlockProposal) extends RoundCommand
case class BroadcastMajorityUnionedBlock(roundId: RoundId, cb: MajorityUnionedBlock) extends RoundCommand
case class ReceivedMajorityUnionedBlock(roundId: RoundId, cb: MajorityUnionedBlock) extends RoundCommand
