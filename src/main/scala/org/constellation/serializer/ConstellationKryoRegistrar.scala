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

    kryo.register(classOf[DownloadRequest])
    kryo.register(classOf[ParentBundleHash])
    kryo.register(classOf[TransactionHash])
    kryo.register(classOf[BatchTXHashRequest])
    kryo.register(classOf[BatchBundleHashRequest])
    kryo.register(classOf[DownloadResponse])
    kryo.register(classOf[PeerSyncHeartbeat])
    kryo.register(classOf[Set[String]])

    kryo.register(classOf[SerializedUDPMessage])
    kryo.register(classOf[HandShakeResponse])
    kryo.register(classOf[HandShakeMessage])
    kryo.register(classOf[HandShake])
    kryo.register(classOf[HandShakeResponseMessage])

    kryo.register(classOf[Id])
    kryo.register(classOf[Peer])
    kryo.register(classOf[Bundle])
    kryo.register(classOf[BundleData])
    kryo.register(classOf[Sheaf])
    kryo.register(classOf[Transaction])
    kryo.register(classOf[TransactionData])

    kryo.register(classOf[EncodedPublicKey])
    kryo.register(classOf[Array[Byte]])

    kryo.register(classOf[Signed[AddressMetaData]])
    kryo.register(classOf[Signed[CounterPartyTXRequest]])
    kryo.register(classOf[Signed[TransactionData]])
    kryo.register(classOf[Signed[Transaction]])
    kryo.register(classOf[Signed[ConflictDetectedData]])
    kryo.register(classOf[Signed[ConflictDetected]])
    kryo.register(classOf[Signed[VoteData]])
    kryo.register(classOf[Signed[VoteDataSimpler]])
    kryo.register(classOf[Signed[Vote]])
    kryo.register(classOf[Signed[BundleBlock]])
    kryo.register(classOf[Signed[BundleData]])


    // These may not be necessary
    kryo.register(classOf[Gossip[Signed[AddressMetaData]]])
    kryo.register(classOf[Gossip[Signed[CounterPartyTXRequest]]])
    kryo.register(classOf[Gossip[Signed[TransactionData]]])
    kryo.register(classOf[Gossip[Signed[Transaction]]])
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
    kryo.register(classOf[Gossip[Peer]])


    kryo.register(classOf[AddressMetaData])


    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("akka.util.ByteString$ByteString1C"))
    kryo.register(Class.forName("scala.None$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))

  }
}
