package org.constellation.schema.v2.edge

import java.security.KeyPair

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.signature.{HashSignature, Signable, SignatureBatch}

/**
  * Encapsulation for all witness information about a given observation edge.
  *
  * @param signatureBatch : Collection of validation signatures about the edge.
  */
case class SignedObservationEdge(signatureBatch: SignatureBatch) extends Signable {

  def withSignatureFrom(keyPair: KeyPair): SignedObservationEdge =
    this.copy(signatureBatch = signatureBatch.withSignatureFrom(keyPair))

  def withSignature(hs: HashSignature): SignedObservationEdge =
    this.copy(signatureBatch = signatureBatch.withSignature(hs))

  def plus(other: SignatureBatch): SignedObservationEdge =
    this.copy(signatureBatch = signatureBatch.plus(other))

  def plus(other: SignedObservationEdge): SignedObservationEdge =
    this.copy(signatureBatch = signatureBatch.plus(other.signatureBatch))

  def baseHash: String = signatureBatch.hash

  override def hash = signatureBatch.hash

}

object SignedObservationEdge {
  implicit val signedObservationEdgeEncoder: Encoder[SignedObservationEdge] = deriveEncoder
  implicit val signedObservationEdgeDecoder: Decoder[SignedObservationEdge] = deriveDecoder
}
