package org.constellation.domain.observation

sealed trait ObservationEvent

// TODO: Add specific fields like consensus round or snapshot hash
case class CheckpointBlockWithMissingParents() extends ObservationEvent

case class RequestTimeoutOnConsensus() extends ObservationEvent

case class RequestTimeoutOnResolving() extends ObservationEvent

case class SnapshotMisalignment() extends ObservationEvent

case class ProposalOfInvalidTransaction() extends ObservationEvent
