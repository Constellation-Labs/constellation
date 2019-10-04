package org.constellation.primitives

import java.security.KeyPair

import org.constellation.domain.schema.Id
import org.constellation.storage.ConsensusObject
import org.constellation.util.Signable
import constellation._

sealed trait ObservationEvent

case class ObservationData(
  id: Id,
  event: ObservationEvent,
  time: Long
) extends Signable

case class Observation(
  signedObservationData: SignedData[ObservationData]
) extends ConsensusObject {
  def hash: String = signedObservationData.data.hash
}

object Observation {

  def create(id: Id, event: ObservationEvent, time: Long)(implicit keyPair: KeyPair): Observation = {
    val data = ObservationData(id, event, time)
    Observation(
      SignedData(data, hashSignBatchZeroTyped(data, keyPair))
    )
  }
}

// TODO: Add specific fields like consensus round or snapshot hash
case class CheckpointBlockWithMissingParents() extends ObservationEvent

case class RequestTimeoutOnConsensus() extends ObservationEvent

case class RequestTimeoutOnResolving() extends ObservationEvent

case class SnapshotMisalignment() extends ObservationEvent

case class ProposalOfInvalidTransaction() extends ObservationEvent
