package org.constellation

import java.security.{KeyPair, PrivateKey, PublicKey}

import com.google.common.hash.Hashing
import org.constellation.keytool.KeyUtils.{bytes2hex, publicKeyToAddressString, publicKeyToHex, signData}
import org.constellation.schema.address.AddressMetaData

package object schema {

  implicit class PublicKeyExt(publicKey: PublicKey) {

    def toId: Id = Id(hex)

    def hex: String = publicKeyToHex(publicKey)
  }

  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    def dataEqual(other: KeyPair): Boolean =
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic

    def address: String = publicKeyToAddressString(kp.getPublic)

    def toId: Id = kp.getPublic.toId

  }

  def hashSerialized(obj: AnyRef) = Kryo.serializeAnyRef(obj).sha256
  def hashSerializedBytes(obj: AnyRef) = Kryo.serializeAnyRef(obj).sha256Bytes

  def signHashWithKey(hash: String, privateKey: PrivateKey): String =
    bytes2hex(signData(hash.getBytes())(privateKey))

  implicit def pubKeyToAddress(key: PublicKey): AddressMetaData =
    AddressMetaData(publicKeyToAddressString(key))

  implicit class KryoSerExt(anyRef: AnyRef) {
    def kryo: Array[Byte] = Kryo.serializeAnyRef(anyRef)
  }

  implicit class SHA256Ext(s: String) {

    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }

  implicit class SHA256ByteExt(arr: Array[Byte]) {

    def sha256: String = Hashing.sha256().hashBytes(arr).toString

    def sha256Bytes: Array[Byte] = Hashing.sha256().hashBytes(arr).asBytes()
  }
}
