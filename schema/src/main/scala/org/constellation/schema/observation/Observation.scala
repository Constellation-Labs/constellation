package org.constellation.schema.observation

import java.security.KeyPair

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema
import org.constellation.schema.consensus.ConsensusObject
import org.constellation.schema.{Id, SignedData}
import org.joda.time.{DateTime, DateTimeUtils}
import org.constellation.schema.signature.SignHelp.hashSignBatchZeroTyped

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
      schema.SignedData(data, hashSignBatchZeroTyped(data, keyPair))
    )
  }
}
