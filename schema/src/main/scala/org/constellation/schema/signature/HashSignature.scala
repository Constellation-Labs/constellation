package org.constellation.schema.signature

import java.security.{PublicKey, SignatureException}

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.verifySignature
import org.constellation.schema.{Id, pubKeyToAddress}
import cats.implicits._

case class HashSignature(
  signature: String,
  id: Id
) extends Ordered[HashSignature] {

  def publicKey: PublicKey = id.toPublicKey

  def address: String = publicKey.address

  def valid(hash: String): Boolean = {
    val verifyResultOrError = for {
      signatureBytes <- Either.catchOnly[NumberFormatException](KeyUtils.hex2bytes(signature))
      verifyResult <- Either.catchOnly[SignatureException](verifySignature(hash.getBytes(), signatureBytes)(publicKey))
    } yield verifyResult

    verifyResultOrError
      .leftMap(_ => false)
      .merge
  }

  override def compare(that: HashSignature): Int =
    signature.compare(that.signature)
}

object HashSignature {
  implicit val hashSignatureEncoder: Encoder[HashSignature] = deriveEncoder
  implicit val hashSignatureDecoder: Decoder[HashSignature] = deriveDecoder
}
