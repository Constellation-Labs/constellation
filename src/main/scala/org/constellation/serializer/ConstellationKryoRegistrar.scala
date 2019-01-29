package org.constellation.serializer

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.constellation.consensus._
import org.constellation.p2p.SerializedUDPMessage
import org.constellation.primitives.Schema._
import org.constellation.primitives.{ChannelMessage, ChannelMessageData, SignedData}
import org.constellation.util.{EncodedPublicKey, HashSignature, SignatureBatch, Signed}

class ConstellationKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit = {
    this.registerClasses(kryo)
  }

  def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[ChannelMessageData])
    kryo.register(classOf[SignedData[ChannelMessageData]])
    kryo.register(classOf[ChannelMessage])
    kryo.register(classOf[StoredSnapshot])
    kryo.register(classOf[SnapshotInfo])
    kryo.register(classOf[TipData])
    kryo.register(classOf[Height])
    kryo.register(classOf[CommonMetadata])
    kryo.register(classOf[Seq[CheckpointBlock]])
    kryo.register(classOf[Address])
    kryo.register(classOf[CheckpointEdge])
    kryo.register(classOf[AddressCacheData])
    kryo.register(classOf[TransactionCacheData])
    kryo.register(classOf[CheckpointCacheData])
    kryo.register(classOf[SignedObservationEdgeCache])
    kryo.register(classOf[Transaction])
    kryo.register(classOf[Edge[Address, Address, TransactionEdgeData]])
    kryo.register(classOf[ResolvedObservationEdge[Address, Address, TransactionEdgeData]])
    kryo.register(classOf[Edge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData]])
    kryo.register(classOf[ResolvedObservationEdge[SignedObservationEdge, SignedObservationEdge, CheckpointEdgeData]])
    kryo.register(classOf[Edge[SignedObservationEdge, SignedObservationEdge, Nothing]])
    kryo.register(classOf[ResolvedObservationEdge[SignedObservationEdge, SignedObservationEdge, Nothing]])
    kryo.register(classOf[SignatureBatch])
    kryo.register(classOf[HashSignature])
    kryo.register(classOf[SignedObservationEdge])
    kryo.register(classOf[ObservationEdge])
    kryo.register(classOf[CheckpointBlock])
    kryo.register(classOf[TypedEdgeHash])
  //  kryo.register(classOf[EdgeHashType])
    kryo.register(classOf[Enumeration#Value])
    kryo.register(classOf[TransactionEdgeData])
    kryo.register(classOf[CheckpointEdgeData])
    kryo.register(classOf[Snapshot])

    kryo.register(classOf[DownloadRequest])
    kryo.register(classOf[ParentBundleHash])
    kryo.register(classOf[TransactionHash])
    kryo.register(classOf[BatchTXHashRequest])
    kryo.register(classOf[BatchBundleHashRequest])
    kryo.register(classOf[Set[String]])

    kryo.register(classOf[SerializedUDPMessage])
    kryo.register(classOf[HandShakeResponse])
    kryo.register(classOf[HandShakeMessage])
    kryo.register(classOf[HandShake])
    kryo.register(classOf[HandShakeResponseMessage])

    kryo.register(classOf[Id])
    kryo.register(classOf[Peer])
    kryo.register(classOf[BundleData])
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
    kryo.register(classOf[Signed[Vote]])
    kryo.register(classOf[Signed[BundleBlock]])
    kryo.register(classOf[Signed[BundleData]])
    kryo.register(classOf[AddressMetaData])

    kryo.register(Class.forName("org.constellation.primitives.Schema$EdgeHashType$"))
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("akka.util.ByteString$ByteString1C"))
    kryo.register(Class.forName("scala.None$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))

  }
}
