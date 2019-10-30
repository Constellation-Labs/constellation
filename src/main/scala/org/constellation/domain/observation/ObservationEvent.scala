package org.constellation.domain.observation

import org.constellation.checkpoint.CheckpointBlockValidator.ValidationResult
import org.constellation.consensus.Consensus.RoundId
import org.constellation.primitives.CheckpointBlock

sealed trait ObservationEvent

// TODO: Add specific fields like consensus round or snapshot hash
case class CheckpointBlockWithMissingParents(checkpointBaseHash: String) extends ObservationEvent
case class CheckpointBlockWithMissingSoe(checkpointBaseHash: String) extends ObservationEvent
case class RequestTimeoutOnConsensus(roundId: RoundId) extends ObservationEvent
case class RequestTimeoutOnResolving(endpoint: String, hashes: List[String]) extends ObservationEvent
case class SnapshotMisalignment() extends ObservationEvent
case class CheckpointBlockInvalid(checkpointBaseHash: String, reason: ValidationResult[CheckpointBlock])
    extends ObservationEvent
