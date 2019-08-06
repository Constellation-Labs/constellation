package org.constellation.primitives

import org.constellation.primitives.Schema.Id
import org.constellation.storage.ConsensusObject

trait Experience extends ConsensusObject {
  def id: Id

  def hash: String = ??? // TODO: ?
}

case class CheckpointBlockWithMissingParents(id: Id) extends Experience
case class RequestTimeoutOnConsensus(id: Id) extends Experience
case class RequestTimeoutOnResolving(id: Id) extends Experience
case class SnapshotMisalignment(id: Id) extends Experience
case class ProposalOfInvalidTransaction(id: Id) extends Experience
