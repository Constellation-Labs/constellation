package org.constellation.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.{DeflateSerializer, EnumNameSerializer, FieldSerializer, JavaSerializer}
import com.twitter.chill.IKryoRegistrar
import org.constellation.consensus._
import org.constellation.domain.observation.{CheckpointBlockInvalid, CheckpointBlockWithMissingParents, CheckpointBlockWithMissingSoe, Observation, ObservationData, RequestTimeoutOnConsensus, RequestTimeoutOnResolving, SnapshotMisalignment}
import org.constellation.p2p.{PeerNotification, SerializedUDPMessage}
import org.constellation.primitives._
import org.constellation.primitives.Schema._
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.schema.Id
import org.constellation.util.{HashSignature, SignatureBatch}
import com.esotericsoftware.minlog.Log

class ConstellationKryoRegistrar extends IKryoRegistrar {

  override def apply(kryo: Kryo): Unit =
    this.registerClasses(kryo)

  def registerClasses(kryo: Kryo): Unit = {
    Log.ERROR
    kryo.register(classOf[ChannelMessageData])
    kryo.register(classOf[SignedData[ChannelMessageData]])
    kryo.register(classOf[ChannelMessage])
    kryo.register(classOf[StoredSnapshot])
    kryo.register(classOf[SnapshotInfo])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[SnapshotInfo])))
    kryo.register(classOf[SnapshotInfoSer])
    kryo.register(classOf[TipData])
    kryo.register(classOf[Height])
    kryo.register(classOf[CommonMetadata])
    kryo.register(classOf[PeerNotification])
//    kryo.register(classOf[Seq.GenericCanBuildFrom[_]])
    kryo.register(classOf[Array[CheckpointBlock]])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[CheckpointBlock]])))
    kryo.register(classOf[Array[CheckpointCache]], new JavaSerializer())//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[CheckpointCache]])))
    kryo.register(classOf[Array[(String, LastTransactionRef)]])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[(String, LastTransactionRef)]])))
    kryo.register(classOf[Array[(String, AddressCacheData)]])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[(String, AddressCacheData)]])))
    kryo.register(classOf[Array[String]])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[String]])))
    kryo.register(classOf[Address])//
    kryo.register(classOf[Int])//
    kryo.register(classOf[CheckpointEdge])
    kryo.register(classOf[AddressCacheData])
    kryo.register(classOf[TransactionCacheData])
    kryo.register(classOf[CheckpointCache], new DeflateSerializer(new FieldSerializer(kryo, classOf[CheckpointCache])))//, new JavaSerializer())//todo try java ser if issue with class registration
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
    kryo.register(classOf[Enumeration#Value])//todo: EnumNameSerializer?
    kryo.register(classOf[TransactionEdgeData])
    kryo.register(classOf[CheckpointEdgeData])
    kryo.register(classOf[Snapshot])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Snapshot])))
//    kryo.register(classOf[Array[Snapshot]])//, new DeflateSerializer(new FieldSerializer(kryo, classOf[Array[Snapshot]])))
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
    kryo.register(classOf[String])

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
