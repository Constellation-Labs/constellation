package org.constellation.serializer

import atb.common.DefaultRandomGenerator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.constellation.consensus._
import org.constellation.domain.observation._
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.p2p.{PeerNotification, SerializedUDPMessage}
import org.constellation.primitives.Schema._
import org.constellation.primitives.{SignedData, _}
import org.constellation.schema.Id
import org.constellation.util.{HashSignature, SignatureBatch}
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer
import org.constellation.primitives.Schema.EdgeHashType.EdgeHashType
import org.constellation.rewards.EigenTrustAgents
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer
import org.constellation.domain.rewards.StoredRewards
import org.constellation.primitives.IPManager.IP
import org.constellation.rewards.EigenTrustAgents

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

class ConstellationKryoRegistrar extends IKryoRegistrar {

  override def apply(kryo: Kryo): Unit =
    this.registerClasses(kryo)

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ChannelMessageData])
    kryo.register(classOf[SignedData[ChannelMessageData]])
    kryo.register(classOf[SignedData[ObservationData]])
    kryo.register(classOf[SignedData[ObservationData]])
    kryo.register(classOf[ChannelMessage])
    kryo.register(classOf[Seq[ChannelMessage]])
    kryo.register(classOf[StoredSnapshot])
    kryo.register(classOf[SnapshotInfo])
    kryo.register(classOf[TipData])
    kryo.register(classOf[Height])
    kryo.register(classOf[Option[Height]])
    kryo.register(classOf[CommonMetadata])
    kryo.register(classOf[Seq[CheckpointBlock]])
    kryo.register(classOf[Seq[Transaction]])
    kryo.register(classOf[SignedData[ObservationData]])
    kryo.register(classOf[Seq[CheckpointCache]])
    kryo.register(classOf[Map[String, AddressCacheData]])
    kryo.register(classOf[Map[String, TipData]])
    kryo.register(classOf[Map[String, LastTransactionRef]])
    kryo.register(classOf[Map[Id, Double]])
    kryo.register(classOf[Map[IP, Id]])
    kryo.register(classOf[SortedMap[Id, Double]])
    kryo.register(classOf[TreeMap[Id, Double]])
    kryo.register(classOf[Seq[(String, AddressCacheData)]])
    kryo.register(classOf[Seq[(String, TipData)]])
    kryo.register(classOf[Seq[(String, LastTransactionRef)]])
    kryo.register(classOf[Seq[(Id, Double)]])
    kryo.register(classOf[Seq[(IP, Id)]])
    kryo.register(classOf[Address])
    kryo.register(classOf[CheckpointEdge])
    kryo.register(classOf[AddressCacheData])
    kryo.register(classOf[TransactionCacheData])
    kryo.register(classOf[CheckpointCache])
    kryo.register(classOf[Transaction], 1001)
    kryo.register(classOf[TransactionGossip])
    kryo.register(classOf[Edge[TransactionEdgeData]], 1005)
    kryo.register(classOf[Edge[CheckpointEdgeData]])
    kryo.register(classOf[SignatureBatch], 1006)
    kryo.register(classOf[HashSignature], 1007)
    kryo.register(classOf[SignedObservationEdge], 1003)
    kryo.register(classOf[ObservationEdge], 1002)
    kryo.register(classOf[CheckpointBlock])
    kryo.register(classOf[PeerNotification])
    kryo.register(classOf[Seq[PeerNotification]])
    kryo.register(classOf[TypedEdgeHash], 1004)
    kryo.register(classOf[Enumeration#Value], 1008)
    kryo.register(classOf[TransactionEdgeData], 1009)
    kryo.register(classOf[CheckpointEdgeData])
    kryo.register(classOf[Snapshot])
    kryo.register(classOf[GenesisObservation])
    kryo.register(classOf[LastTransactionRef], 1010)
    kryo.register(classOf[ObservationData])
    kryo.register(classOf[CheckpointBlockWithMissingParents])
    kryo.register(classOf[CheckpointBlockWithMissingSoe])
    kryo.register(classOf[RequestTimeoutOnConsensus])
    kryo.register(classOf[RequestTimeoutOnResolving])
    kryo.register(classOf[CheckpointBlockInvalid])
    kryo.register(classOf[Observation])
    kryo.register(classOf[Seq[Observation]])
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[String]])
    kryo.register(classOf[Seq[String]])
    kryo.register(classOf[Set[String]])
    kryo.register(classOf[Seq[String]])

    kryo.register(classOf[SerializedUDPMessage])
    kryo.register(classOf[Id], 1011)

    kryo.register(classOf[Array[Byte]], 1012)
    kryo.register(classOf[Array[Array[Byte]]])
    kryo.register(classOf[Option[Long]], 1013)
    kryo.register(classOf[String], 1014)
    kryo.register(classOf[Boolean], 1015)

    // EigenTrustJ
    kryo.register(classOf[DefaultRandomGenerator])
    kryo.register(classOf[MersenneTwister])
    kryo.register(classOf[cern.jet.random.Normal])
    kryo.register(classOf[cern.jet.random.Uniform])
    kryo.register(Class.forName("[[I")) // To make int[][] serializable
    kryo.register(classOf[EigenTrustJ])
    kryo.register(classOf[EigenTrustAgents])
    kryo.register(classOf[StoredRewards])

    kryo.register(classOf[AddressMetaData])

    kryo.register(Class.forName("scala.math.LowPriorityOrderingImplicits$$anon$3"))
    kryo.register(Class.forName("scala.Predef$$anon$2"))
    kryo.register(scala.math.Ordering.String.getClass)

    kryo.register(EdgeHashType.getClass, 1016)

    kryo.register(Class.forName("scala.Enumeration$Val"), 1017)
    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"), 1018)
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"), 1019)
    kryo.register(Class.forName("scala.collection.IndexedSeqLike$Elements"))
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"), 1020)
    kryo.register(Class.forName("akka.util.ByteString$ByteString1C"))
    kryo.register(Class.forName("scala.None$"), 1021)
    kryo.register(Class.forName("scala.collection.immutable.Nil$"), 1022)
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"), 1023)

  }
}
