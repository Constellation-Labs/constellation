package org.constellation.schema.signature

import org.constellation.keytool.KeyUtils

import org.constellation.schema._

trait Signable {

  protected def toEncode: AnyRef = this

  /**
    * Override this value with false if data representing this class is also serialized and deserialized in format other
    * than Kryo (i.e. JSON for transmission over the network).
    */
  def trackRefs: Boolean = true

  def signInput: Array[Byte] = hash.getBytes()

  def hash: String = hashSerialized(getEncoding, trackRefs)

  def short: String = hash.slice(0, 5)

  def getEncoding: String = hashSerialized(toEncode, trackRefs)

  def getHexEncoding = KeyUtils.bytes2hex(hashSerializedBytes(toEncode, trackRefs))

  def runLengthEncoding(hashes: String*): String = hashes.fold("")((acc, hash) => s"$acc${hash.length}$hash")

}
