package org.constellation.primitives

import org.constellation.primitives.Schema.Id
import org.constellation.storage.ConsensusObject

trait Experience extends ConsensusObject {
  def id: Id
  def date: Long

  def hash: String = ???
}

case class CheckpointBlockWithMissingParents(id: Id, date: Long) extends Experience

object CheckpointBlockWithMissingParents {
  def apply(id: Id) = new CheckpointBlockWithMissingParents(id, System.currentTimeMillis())
}

case class RequestTimeoutOnConsensus(id: Id, date: Long) extends Experience

object RequestTimeoutOnConsensus {
  def apply(id: Id) = new RequestTimeoutOnConsensus(id, System.currentTimeMillis())
}
case class RequestTimeoutOnResolving(id: Id, date: Long) extends Experience

object RequestTimeoutOnResolving {
  def apply(id: Id) = new RequestTimeoutOnResolving(id, System.currentTimeMillis())
}

case class SnapshotMisalignment(id: Id, date: Long) extends Experience

object SnapshotMisalignment {
  def apply(id: Id) = new SnapshotMisalignment(id, System.currentTimeMillis())
}
case class ProposalOfInvalidTransaction(id: Id, date: Long) extends Experience

object ProposalOfInvalidTransaction {
  def apply(id: Id) = new ProposalOfInvalidTransaction(id, System.currentTimeMillis())
}
