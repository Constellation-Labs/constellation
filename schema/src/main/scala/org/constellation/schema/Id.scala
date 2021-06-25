package org.constellation.schema

import java.security.PublicKey

import com.google.common.hash.Hashing
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.circe.generic.semiauto._
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.hexToPublicKey

case class Id(hex: String) extends Ordered[Id] {

  @transient
  lazy val short: String = hex.slice(0, 5)

  @transient
  lazy val medium: String = hex.slice(0, 10)

  @transient
  lazy val address: String = KeyUtils.publicKeyToAddressString(toPublicKey)

  @transient
  lazy val toPublicKey: PublicKey = hexToPublicKey(hex)

  @transient
  lazy val bytes: Array[Byte] = KeyUtils.hex2bytes(hex)

  @transient
  lazy val bigInt: BigInt = BigInt(bytes)

  @transient
  lazy val distance: BigInt = BigInt(Hashing.sha256.hashBytes(toPublicKey.getEncoded).asBytes())

  override def compare(that: Id): Int = hex.compare(that.hex)
}

object Id {

  def fromPublicKey(publicKey: PublicKey): Id = Id(KeyUtils.publicKeyToHex(publicKey))

  implicit val idEncoder: Encoder[Id] = deriveEncoder
  implicit val idDecoder: Decoder[Id] = deriveDecoder

  implicit val keyIdEncoder: KeyEncoder[Id] = KeyEncoder.encodeKeyString.contramap[Id](_.hex)
  implicit val keyIdDecoder: KeyDecoder[Id] = KeyDecoder.decodeKeyString.map(Id(_))
}
