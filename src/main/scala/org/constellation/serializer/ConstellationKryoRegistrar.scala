package org.constellation.serializer

import java.security.PublicKey

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey
import org.constellation.p2p.SerializedUDPMessage
import org.constellation.primitives.Schema._
import org.constellation.util.{EncodedPublicKey, Signed}

class ConstellationKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit = {
    this.registerClasses(kryo)
  }

  def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[EncodedPublicKey])
    kryo.register(classOf[Array[Byte]])

    kryo.register(classOf[Signed[Address]])
    kryo.register(classOf[Signed[CounterPartyTXRequest]])
    kryo.register(classOf[Signed[TXData]])
    kryo.register(classOf[Signed[TX]])

    kryo.register(classOf[Signed[ConflictDetectedData]])

    kryo.register(classOf[Signed[ConflictDetected]])
    kryo.register(classOf[Signed[VoteData]])
    kryo.register(classOf[Signed[VoteDataSimpler]])
    kryo.register(classOf[Signed[Vote]])
    kryo.register(classOf[Signed[BundleBlock]])
    kryo.register(classOf[Signed[BundleData]])

    kryo.register(classOf[Id])

    kryo.register(classOf[HandShake])
    kryo.register(classOf[HandShakeResponseMessage])

    kryo.register(classOf[Peer])

    kryo.register(classOf[Bundle])

    kryo.register(classOf[BundleData])

    kryo.register(classOf[TX])

    kryo.register(classOf[TXData])

    kryo.register(classOf[Gossip[Signed[Address]]])
    kryo.register(classOf[Gossip[Signed[CounterPartyTXRequest]]])
    kryo.register(classOf[Gossip[Signed[TXData]]])
    kryo.register(classOf[Gossip[Signed[TX]]])

    kryo.register(classOf[Gossip[Signed[ConflictDetectedData]]])

    kryo.register(classOf[Gossip[Signed[ConflictDetected]]])
    kryo.register(classOf[Gossip[Signed[VoteData]]])
    kryo.register(classOf[Gossip[Signed[VoteDataSimpler]]])
    kryo.register(classOf[Gossip[Signed[Vote]]])
    kryo.register(classOf[Gossip[Signed[BundleBlock]]])
    kryo.register(classOf[Gossip[Signed[BundleData]]])

    kryo.register(classOf[Gossip[RequestBundleData]])

    kryo.register(classOf[Gossip[PeerSyncHeartbeat]])
    kryo.register(classOf[Gossip[Bundle]])
    kryo.register(classOf[Gossip[DownloadRequest]])
    kryo.register(classOf[Gossip[DownloadResponse]])
    kryo.register(classOf[Gossip[SyncData]])

    kryo.register(classOf[Gossip[MissingTXProof]])
    kryo.register(classOf[Gossip[RequestTXProof]])

    kryo.register(classOf[Gossip[HandShake]])
    kryo.register(classOf[Gossip[HandShakeResponseMessage]])

    kryo.register(classOf[HandShakeResponse])

    kryo.register(classOf[Gossip[Peer]])

    kryo.register(classOf[SerializedUDPMessage])

    kryo.register(classOf[Address])
    kryo.register(classOf[HandShakeMessage])

    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("akka.util.ByteString$ByteString1C"))
    kryo.register(Class.forName("scala.None$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))

  }
}
