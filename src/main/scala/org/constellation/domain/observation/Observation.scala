package org.constellation.domain.observation

import java.security.KeyPair

import constellation._
import org.constellation.domain.consensus.ConsensusObject
import org.constellation.primitives.SignedData
import org.constellation.schema.{HashGenerator, Id}
import org.joda.time.DateTimeUtils

case class Observation(
  signedObservationData: SignedData[ObservationData]
) extends ConsensusObject {
  def hash: String = signedObservationData.data.hash
}

object Observation {

  def create(id: Id, event: ObservationEvent, time: Long = DateTimeUtils.currentTimeMillis())(
    implicit keyPair: KeyPair,
    hashGenerator: HashGenerator
  ): Observation = {
    val data = ObservationData(id, event, time)
    Observation(
      SignedData(data, hashSignBatchZeroTyped(data, keyPair))
    )
  }
}
