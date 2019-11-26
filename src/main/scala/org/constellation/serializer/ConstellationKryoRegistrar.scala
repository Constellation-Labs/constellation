package org.constellation.serializer

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.constellation.consensus._
import org.constellation.domain.observation.{
  CheckpointBlockInvalid,
  CheckpointBlockWithMissingParents,
  CheckpointBlockWithMissingSoe,
  Observation,
  ObservationData,
  RequestTimeoutOnConsensus,
  RequestTimeoutOnResolving,
  SnapshotMisalignment
}
import org.constellation.p2p.SerializedUDPMessage
import org.constellation.primitives._
import org.constellation.primitives.Schema._
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.schema.Id
import org.constellation.util.{HashSignature, SignatureBatch}

class ConstellationKryoRegistrar extends IKryoRegistrar {

  override def apply(kryo: Kryo): Unit =
    this.registerClasses(kryo)

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
    kryo.register(classOf[CheckpointCache])
    kryo.register(classOf[SignedObservationEdgeCache])
    kryo.register(classOf[Transaction])
    kryo.register(classOf[TransactionGossip])
    kryo.register(classOf[Edge[TransactionEdgeData]])
    kryo.register(classOf[Edge[CheckpointEdgeData]])
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
    kryo.register(classOf[GenesisObservation])
    kryo.register(classOf[LastTransactionRef])
    kryo.register(classOf[ObservationData])
    kryo.register(classOf[CheckpointBlockWithMissingParents])
    kryo.register(classOf[CheckpointBlockWithMissingSoe])
    kryo.register(classOf[RequestTimeoutOnConsensus])
    kryo.register(classOf[RequestTimeoutOnResolving])
    kryo.register(classOf[SnapshotMisalignment])
    kryo.register(classOf[CheckpointBlockInvalid])
    kryo.register(classOf[Observation])

    kryo.register(classOf[Set[String]])

    kryo.register(classOf[SerializedUDPMessage])
    kryo.register(classOf[Id])

    kryo.register(classOf[Array[Byte]])

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
