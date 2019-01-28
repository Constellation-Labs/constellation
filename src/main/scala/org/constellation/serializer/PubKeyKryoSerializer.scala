package org.constellation.serializer

import java.security.PublicKey
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.Kryo

import org.constellation.crypto.KeyUtils

/** Encoded public key wrapper. */
case class EncodedPubKey(pubKeyEncoded: Array[Byte])

/** Public key serializer with reader and writer. */
class PubKeyKryoSerializer extends Serializer[PublicKey] {

  /** Write method. */
  override def write(kryoI: Kryo, output: Output, `object`: PublicKey): Unit = {

    val enc = `object`.getEncoded
    kryoI.writeClassAndObject(output, EncodedPubKey(enc))
  }

  /** Read method.
    *
    * @return Public key.
    */
  override def read(kryoI: Kryo, input: Input, `type`: Class[PublicKey]): PublicKey = {

    val encP = EncodedPubKey(null)
    kryoI.reference(encP)

    val enc = kryoI.readClassAndObject(input).asInstanceOf[EncodedPubKey]
    KeyUtils.bytesToPublicKey(enc.pubKeyEncoded)
  }

} // end class PubKeyKryoSerializer
