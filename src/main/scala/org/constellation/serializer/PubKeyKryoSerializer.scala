package org.constellation.serializer

import java.security.PublicKey
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.Kryo

import org.constellation.crypto.KeyUtils

/** Documentation. */
case class EncodedPubKey(pubKeyEncoded: Array[Byte])

/** Documentation. */
class PubKeyKryoSerializer extends Serializer[PublicKey] {

  /** Documentation. */
  override def write(kryoI: Kryo, output: Output, `object`: PublicKey): Unit = {
    val enc = `object`.getEncoded
    kryoI.writeClassAndObject(output, EncodedPubKey(enc))
  }

  /** Documentation. */
  override def read(kryoI: Kryo, input: Input, `type`: Class[PublicKey]): PublicKey = {
    val encP = EncodedPubKey(null)
    kryoI.reference(encP)
    val enc = kryoI.readClassAndObject(input).asInstanceOf[EncodedPubKey]
    KeyUtils.bytesToPublicKey(enc.pubKeyEncoded)
  }
}
