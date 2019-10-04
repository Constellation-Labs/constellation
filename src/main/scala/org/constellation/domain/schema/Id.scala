package org.constellation.domain.schema

import java.security.PublicKey

import com.google.common.hash.Hashing
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils.hexToPublicKey

case class Id(hex: String) {

  @transient
  val short: String = hex.toString.slice(0, 5)

  @transient
  val medium: String = hex.toString.slice(0, 10)

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

}
