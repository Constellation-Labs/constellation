package org.constellation.serializer

import atb.common.DefaultRandomGenerator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.constellation.consensus._
import org.constellation.domain.observation._
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.Schema._
import org.constellation.primitives.{SignedData, _}
import org.constellation.schema.Id
import org.constellation.util.{HashSignature, SignatureBatch}
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer
import org.constellation.primitives.Schema.EdgeHashType
import org.constellation.rewards.EigenTrustAgents
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer
import org.constellation.domain.rewards.StoredRewards
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.primitives.IPManager.IP
import org.constellation.rewards.EigenTrustAgents
import org.constellation.session.Registration.JoinRequestPayload

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

class ConstellationKryoRegistrar extends IKryoRegistrar {
  private val printRegistrationIDs = false

  override def apply(kryo: Kryo): Unit = {
    this.registerClasses(kryo)

    if (printRegistrationIDs) {
      val nextId = kryo.getNextRegistrationId
      println(s"Next ID: $nextId")

      (0 until nextId).foreach { i =>
        val registration = kryo.getRegistration(i)
        println(s"ID: ${registration.getId}, ${registration.getType.getName}")
      }
    }
  }

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ChannelMessageData], 145)
    kryo.register(classOf[SignedData[ChannelMessageData]], 146)
    kryo.register(classOf[SignedData[ObservationData]]) // todo
    kryo.register(classOf[SignedData[ObservationData]]) // todo
    kryo.register(classOf[ChannelMessage], 147)
    kryo.register(classOf[Seq[ChannelMessage]], 148)
    kryo.register(classOf[StoredSnapshot], 149)
    kryo.register(classOf[SnapshotInfo], 150)
    kryo.register(classOf[TipData], 151)
    kryo.register(classOf[Height], 152)
    kryo.register(classOf[Option[Height]], 153)
    kryo.register(classOf[CommonMetadata], 154)
    kryo.register(classOf[Seq[CheckpointBlock]]) // todo
    kryo.register(classOf[Seq[Transaction]]) // todo
    kryo.register(classOf[SignedData[ObservationData]]) // todo
    kryo.register(classOf[Seq[CheckpointCache]]) // todo
    kryo.register(classOf[Map[String, AddressCacheData]], 155)
    kryo.register(classOf[Map[String, TipData]]) // todo
    kryo.register(classOf[Map[String, LastTransactionRef]]) // todo
    kryo.register(classOf[Map[Id, Double]]) // todo
    kryo.register(classOf[Map[IP, Id]]) // todo
    kryo.register(classOf[SortedMap[Id, Double]], 156)
    kryo.register(classOf[TreeMap[Id, Double]]) // todo
    kryo.register(classOf[Seq[(String, AddressCacheData)]]) // todo
    kryo.register(classOf[Seq[(String, TipData)]]) // todo
    kryo.register(classOf[Seq[(String, LastTransactionRef)]]) // todo
    kryo.register(classOf[Seq[(Id, Double)]]) // todo
    kryo.register(classOf[Seq[(IP, Id)]]) // todo
    kryo.register(classOf[Address], 157)
    kryo.register(classOf[CheckpointEdge], 158)
    kryo.register(classOf[AddressCacheData], 159)
    kryo.register(classOf[TransactionCacheData], 160)
    kryo.register(classOf[CheckpointCache], 161)
    kryo.register(classOf[Transaction], 1001)
    kryo.register(classOf[TransactionGossip], 162)
    kryo.register(classOf[Edge[TransactionEdgeData]], 1005)
    kryo.register(classOf[Edge[CheckpointEdgeData]])
    kryo.register(classOf[SignatureBatch], 1006)
    kryo.register(classOf[HashSignature], 1007)
    kryo.register(classOf[SignedObservationEdge], 1003)
    kryo.register(classOf[ObservationEdge], 1002)
    kryo.register(classOf[CheckpointBlock], 163)
    kryo.register(classOf[PeerNotification], 164)
    kryo.register(classOf[Seq[PeerNotification]])
    kryo.register(classOf[TypedEdgeHash], 1004)
    kryo.register(classOf[Enumeration#Value], 1008)
    kryo.register(classOf[TransactionEdgeData], 1009)
    kryo.register(classOf[CheckpointEdgeData], 165)
    kryo.register(classOf[Snapshot], 166)
    kryo.register(classOf[GenesisObservation], 167)
    kryo.register(classOf[LastTransactionRef], 1010)
    kryo.register(classOf[ObservationData], 168)
    kryo.register(classOf[CheckpointBlockWithMissingParents], 169)
    kryo.register(classOf[CheckpointBlockWithMissingSoe], 170)
    kryo.register(classOf[RequestTimeoutOnConsensus], 171)
    kryo.register(classOf[RequestTimeoutOnResolving], 172)
    kryo.register(classOf[CheckpointBlockInvalid], 173)
    kryo.register(classOf[Observation], 174)
    kryo.register(classOf[Seq[Observation]]) // todo
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[String]], 175)
    kryo.register(classOf[Seq[String]]) // todo
    kryo.register(classOf[Set[String]], 176)

    kryo.register(classOf[Id], 1011)

    kryo.register(classOf[Array[Byte]], 1012)
    kryo.register(classOf[Array[Array[Byte]]], 177)
    kryo.register(classOf[Option[Long]], 1013)
    kryo.register(classOf[String], 1014)
    kryo.register(classOf[Boolean], 1015)

    // EigenTrustJ
    kryo.register(classOf[DefaultRandomGenerator], 178)
    kryo.register(classOf[MersenneTwister], 179)
    kryo.register(classOf[cern.jet.random.Normal], 180)
    kryo.register(classOf[cern.jet.random.Uniform], 181)
    kryo.register(Class.forName("[[I"), 182) // To make int[][] serializable
    kryo.register(classOf[EigenTrustJ], 183)
    kryo.register(classOf[EigenTrustAgents], 184)
    kryo.register(classOf[StoredRewards], 185)

    kryo.register(classOf[AddressMetaData], 186)
    kryo.register(classOf[BuildInfoJson], 187)

    kryo.register(Class.forName("scala.math.LowPriorityOrderingImplicits$$anon$3"), 188)
    kryo.register(Class.forName("scala.Predef$$anon$2"), 189)
    kryo.register(scala.math.Ordering.String.getClass) // todo

    kryo.register(EdgeHashType.AddressHash.getClass, 1024)
    kryo.register(EdgeHashType.CheckpointDataHash.getClass, 1025)
    kryo.register(EdgeHashType.CheckpointHash.getClass, 1026)
    kryo.register(EdgeHashType.TransactionDataHash.getClass, 1027)
    kryo.register(EdgeHashType.TransactionHash.getClass, 1028)
    kryo.register(EdgeHashType.ValidationHash.getClass, 1029)
    kryo.register(EdgeHashType.BundleDataHash.getClass, 1030)
    kryo.register(EdgeHashType.ChannelMessageDataHash.getClass, 1031)

    kryo.register(Class.forName("scala.Enumeration$Val"), 1017)
    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"), 135)
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"), 131)
    kryo.register(Class.forName("scala.collection.IndexedSeqLike$Elements"), 190)
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"), 1020)
    kryo.register(Class.forName("scala.None$"), 114)
    kryo.register(Class.forName("scala.collection.immutable.Nil$"), 116)
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"), 136)

  }
}
