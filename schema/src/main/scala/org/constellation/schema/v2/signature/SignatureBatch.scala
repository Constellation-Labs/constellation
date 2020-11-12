package org.constellation.schema.v2.signature

import java.security.KeyPair

import cats.kernel.Monoid
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.ProtoAutoCodecs
import org.constellation.schema.v2.signature.SignHelp.hashSign

case class SignatureBatch(
  hash: String,
  signatures: Seq[HashSignature]
) extends Monoid[SignatureBatch] {

  def valid: Boolean =
    signatures.forall(_.valid(hash))

  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  // This is unsafe

  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)

  def withSignatureFrom(other: KeyPair): SignatureBatch =
    withSignature(hashSign(hash, other))

  def plus(other: SignatureBatch): SignatureBatch = {
    val toAdd = other.signatures
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.id).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this.copy(
      signatures = unique
    )
  }

  def withSignature(hs: HashSignature): SignatureBatch = {
    val toAdd = Seq(hs)
    val newSignatures = (signatures ++ toAdd).distinct
    val unique = newSignatures.groupBy(_.id).map { _._2.maxBy(_.signature) }.toSeq.sorted
    this
      .copy(
        signatures = unique
      )
  }

}

object SignatureBatch extends ProtoAutoCodecs[org.constellation.schema.proto.signature.SignatureBatch, SignatureBatch] {
  val cmp = org.constellation.schema.proto.signature.SignatureBatch

  implicit val signatureBatchEncoder: Encoder[SignatureBatch] = deriveEncoder
  implicit val signatureBatchDecoder: Decoder[SignatureBatch] = deriveDecoder
}
