package org.constellation.schema

import java.security.PublicKey

import com.google.common.hash.Hashing
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.hexToPublicKey

case class Id(hex: String) extends Ordered[Id] {

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

  override def compare(that: Id): Int = hex.compare(that.hex)
}
