package org.constellation.schema.serialization

import org.constellation.schema.address.{Address, AddressCacheData, AddressMetaData}
import org.constellation.schema.checkpoint._
import org.constellation.schema.edge._
import org.constellation.schema.observation._
import org.constellation.schema.signature.{HashSignature, SignatureBatch}
import org.constellation.schema.snapshot.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.schema.transaction._
import org.constellation.schema.{
  ChannelMessage,
  ChannelMessageData,
  CommonMetadata,
  GenesisObservation,
  Height,
  Id,
  PeerNotification,
  SignedData
}

import scala.collection.SortedMap

object SchemaKryoRegistrar
    extends ExplicitKryoRegistrar(
      Set(
        (classOf[ChannelMessageData], 145),
        (classOf[SignedData[ChannelMessageData]], 146),
        (classOf[ChannelMessage], 147),
        (classOf[Seq[ChannelMessage]], 148),
        (classOf[StoredSnapshot], 149),
        (classOf[SnapshotInfo], 150),
        (classOf[TipData], 151),
        (classOf[Height], 152),
        (classOf[Option[Height]], 153),
        (classOf[CommonMetadata], 154),
        (classOf[Map[String, AddressCacheData]], 155),
        (classOf[SortedMap[Id, Double]], 156),
        (classOf[Address], 157),
        (classOf[CheckpointEdge], 158),
        (classOf[AddressCacheData], 159),
        (classOf[TransactionCacheData], 160),
        (classOf[CheckpointCache], 161),
        (classOf[Transaction], 1001),
        (classOf[TransactionGossip], 162),
        (classOf[Edge[TransactionEdgeData]], 1005),
        (classOf[SignatureBatch], 1006),
        (classOf[HashSignature], 1007),
        (classOf[SignedObservationEdge], 1003),
        (classOf[ObservationEdge], 1002),
        (classOf[CheckpointBlock], 163),
        (classOf[PeerNotification], 164),
        (classOf[TypedEdgeHash], 1004),
        (classOf[Enumeration#Value], 1008),
        (classOf[TransactionEdgeData], 1009),
        (classOf[CheckpointEdgeData], 165),
        (classOf[Snapshot], 166),
        (classOf[GenesisObservation], 167),
        (classOf[LastTransactionRef], 1010),
        (classOf[ObservationData], 168),
        (classOf[CheckpointBlockWithMissingParents], 169),
        (classOf[CheckpointBlockWithMissingSoe], 170),
        (classOf[RequestTimeoutOnConsensus], 171),
        (classOf[RequestTimeoutOnResolving], 172),
        (classOf[CheckpointBlockInvalid], 173),
        (classOf[Observation], 174),
        (classOf[scala.collection.mutable.ArrayBuffer[String]], 175),
        (classOf[Set[String]], 176),
        (classOf[Id], 1011),
        (classOf[Array[Byte]], 1012),
        (classOf[Array[Array[Byte]]], 177),
        (classOf[Option[Long]], 1013),
        (classOf[String], 1014),
        (classOf[Boolean], 1015),
        (Class.forName("[[I"), 182), // To make int[][] serializable
        (classOf[AddressMetaData], 186),
        (Class.forName("scala.math.LowPriorityOrderingImplicits$$anon$3"), 188),
        (Class.forName("scala.Predef$$anon$2"), 189),
        (EdgeHashType.AddressHash.getClass, 1024),
        (EdgeHashType.CheckpointDataHash.getClass, 1025),
        (EdgeHashType.CheckpointHash.getClass, 1026),
        (EdgeHashType.TransactionDataHash.getClass, 1027),
        (EdgeHashType.TransactionHash.getClass, 1028),
        (EdgeHashType.ValidationHash.getClass, 1029),
        (EdgeHashType.BundleDataHash.getClass, 1030),
        (EdgeHashType.ChannelMessageDataHash.getClass, 1031),
        (Class.forName("scala.Enumeration$Val"), 1017),
        (Class.forName("scala.collection.immutable.HashSet$HashSet1"), 135),
        (Class.forName("scala.collection.immutable.Set$EmptySet$"), 131),
        (Class.forName("scala.collection.IndexedSeqLike$Elements"), 190),
        (Class.forName("scala.collection.immutable.$colon$colon"), 1020),
        (Class.forName("scala.None$"), 114),
        (Class.forName("scala.collection.immutable.Nil$"), 116),
        (Class.forName("scala.collection.immutable.Map$EmptyMap$"), 136)
      )
    ) {}
