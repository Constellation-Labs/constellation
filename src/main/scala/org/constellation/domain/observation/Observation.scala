package org.constellation.domain.observation

import java.security.KeyPair

import org.constellation.domain.consensus.ConsensusObject
import constellation._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.primitives.SignedData
import org.constellation.schema.Id
import org.joda.time.{DateTime, DateTimeUtils}

case class Observation(
  signedObservationData: SignedData[ObservationData]
) extends ConsensusObject {
  def hash: String = signedObservationData.data.hash
}

object Observation {
  implicit val signedObservationDataEncoder: Encoder[SignedData[ObservationData]] = deriveEncoder
  implicit val signedObservationDataDecoder: Decoder[SignedData[ObservationData]] = deriveDecoder

  implicit val observationEncoder: Encoder[Observation] = deriveEncoder
  implicit val observationDecoder: Decoder[Observation] = deriveDecoder

  def create(id: Id, event: ObservationEvent, time: Long = DateTimeUtils.currentTimeMillis())(
    implicit keyPair: KeyPair
  ): Observation = {
    val data = ObservationData(id, event, time)
    Observation(
      SignedData(data, hashSignBatchZeroTyped(data, keyPair))
    )
  }
}
