package org.constellation.serializer

import java.security.PublicKey

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar

class ConstellationKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit = {
    this.registerClasses(kryo)
  }

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[PublicKey], new PubKeyKryoSerializer())
    kryo.addDefaultSerializer(classOf[PublicKey], new PubKeyKryoSerializer())

  //  instance.register(classOf[SerializedUDPMessage], new SerializedUDPMessageSerializer())

  }
}
