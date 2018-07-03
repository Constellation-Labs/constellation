package org.constellation.serializer

import java.security.PublicKey

import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.Kryo
import org.constellation.crypto.KeyUtils

case class EncodedPubKey(pubKeyEncoded: Array[Byte])

class PubKeyKryoSerializer extends Serializer[PublicKey] {
  override def write(kryoI: Kryo, output: Output, `object`: PublicKey): Unit = {
    val enc = `object`.getEncoded
    kryoI.writeClassAndObject(output, EncodedPubKey(enc))
  }

  override def read(kryoI: Kryo, input: Input, `type`: Class[PublicKey]): PublicKey = {
    val encP = EncodedPubKey(null)
    kryoI.reference(encP)
    val enc = kryoI.readClassAndObject(input).asInstanceOf[EncodedPubKey]
    KeyUtils.bytesToPublicKey(enc.pubKeyEncoded)
  }
}
