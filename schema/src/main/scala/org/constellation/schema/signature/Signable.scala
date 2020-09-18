package org.constellation.schema.signature

import org.constellation.keytool.KeyUtils

import org.constellation.schema._

trait Signable {

  protected def toEncode: AnyRef = this

  def signInput: Array[Byte] = hash.getBytes()

  def hash: String = hashSerialized(getEncoding)

  def short: String = hash.slice(0, 5)

  def getEncoding: String = hashSerialized(toEncode)

  def getHexEncoding = KeyUtils.bytes2hex(hashSerializedBytes(toEncode))

  def runLengthEncoding(hashes: String*): String = hashes.fold("")((acc, hash) => s"$acc${hash.length}$hash")

}
