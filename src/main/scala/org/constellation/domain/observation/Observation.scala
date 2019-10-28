package org.constellation.domain.observation

import java.security.KeyPair

import org.constellation.domain.consensus.ConsensusObject
import org.constellation.domain.schema.Id
import constellation._
import org.constellation.primitives.SignedData
import org.joda.time.{DateTime, DateTimeUtils}

case class Observation(
  signedObservationData: SignedData[ObservationData]
) extends ConsensusObject {
  def hash: String = signedObservationData.data.hash
}

object Observation {

  def create(id: Id, event: ObservationEvent, time: Long = DateTimeUtils.currentTimeMillis())(
    implicit keyPair: KeyPair
  ): Observation = {
    val data = ObservationData(id, event, time)
    Observation(
      SignedData(data, hashSignBatchZeroTyped(data, keyPair))
    )
  }
}
