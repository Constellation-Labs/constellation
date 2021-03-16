package org.constellation.schema.signature

import java.security.KeyPair

import cats.Show
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

/**
  * Container for signature and signed value
  */
case class Signed[A <: Signable](signature: HashSignature, value: A) {
  def validSignature: Boolean = signature.valid(value.hash)
}

object Signed {

  def signed[A <: Signable](value: A, kp: KeyPair): Signed[A] = Signed(SignHelp.hashSign(value.hash, kp), value)

  implicit def signedEncoder[A <: Signable](implicit ev: Encoder[A]): Encoder[Signed[A]] = deriveEncoder

  implicit def signedDecoder[A <: Signable](implicit ev: Decoder[A]): Decoder[Signed[A]] = deriveDecoder
}
