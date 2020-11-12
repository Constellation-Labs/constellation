package org.constellation.schema.v2.signature

import java.security.PublicKey

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.verifySignature
import org.constellation.schema.v2.Id

case class HashSignature(
  signature: String,
  id: Id
) extends Ordered[HashSignature] {

  def publicKey: PublicKey = id.toPublicKey

  def address: String = KeyUtils.publicKeyToAddressString(publicKey)

  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)

  override def compare(that: HashSignature): Int =
    signature.compare(that.signature)
}

object HashSignature {
  implicit val hashSignatureEncoder: Encoder[HashSignature] = deriveEncoder
  implicit val hashSignatureDecoder: Decoder[HashSignature] = deriveDecoder
}
