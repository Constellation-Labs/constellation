package org.constellation.schema.serialization

import org.constellation.schema.address.{Address, AddressCacheData, AddressMetaData}
import org.constellation.schema.checkpoint._
import org.constellation.schema.edge._
import org.constellation.schema.observation._
import org.constellation.schema.serialization.ExplicitKryoRegistrar.KryoSerializer.{DefaultSerializer, CompatibleSerializer}
import org.constellation.schema.signature.{HashSignature, SignatureBatch, Signed}
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo, Snapshot, SnapshotInfo, SnapshotInfoV1, SnapshotProposal, SnapshotProposalPayload, StoredSnapshot}
import org.constellation.schema.transaction._
import org.constellation.schema.{ChannelMessage, ChannelMessageData, CommonMetadata, GenesisObservation, Height, Id, PeerNotification, SignedData}

import scala.collection.SortedMap

object SchemaKryoRegistrar
    extends ExplicitKryoRegistrar(
      Set(
        (classOf[ChannelMessageData], 145, DefaultSerializer),
        (classOf[SignedData[ChannelMessageData]], 146, DefaultSerializer),
        (classOf[ChannelMessage], 147, DefaultSerializer),
        (classOf[Seq[ChannelMessage]], 148, DefaultSerializer),
        (classOf[StoredSnapshot], 149, DefaultSerializer),
        (classOf[SnapshotInfoV1], 150, DefaultSerializer),
        (classOf[TipData], 151, DefaultSerializer),
        (classOf[Height], 152, DefaultSerializer),
        (classOf[Option[Height]], 153, DefaultSerializer),
        (classOf[CommonMetadata], 154, DefaultSerializer),
        (classOf[Map[String, AddressCacheData]], 155, DefaultSerializer),
        (classOf[SortedMap[Id, Double]], 156, DefaultSerializer),
        (classOf[Address], 157, DefaultSerializer),
        (classOf[CheckpointEdge], 158, DefaultSerializer),
        (classOf[AddressCacheData], 159, DefaultSerializer),
        (classOf[TransactionCacheData], 160, DefaultSerializer),
        (classOf[CheckpointCache], 161, DefaultSerializer),
        (classOf[Transaction], 1001, DefaultSerializer),
        (classOf[TransactionGossip], 162, DefaultSerializer),
        (classOf[Edge[TransactionEdgeData]], 1005, DefaultSerializer),
        (classOf[SignatureBatch], 1006, DefaultSerializer),
        (classOf[HashSignature], 1007, DefaultSerializer),
        (classOf[SignedObservationEdge], 1003, DefaultSerializer),
        (classOf[ObservationEdge], 1002, DefaultSerializer),
        (classOf[CheckpointBlock], 163, DefaultSerializer),
        (classOf[PeerNotification], 164, DefaultSerializer),
        (classOf[TypedEdgeHash], 1004, DefaultSerializer),
        (classOf[Enumeration#Value], 1008, DefaultSerializer),
        (classOf[TransactionEdgeData], 1009, DefaultSerializer),
        (classOf[CheckpointEdgeData], 165, DefaultSerializer),
        (classOf[Snapshot], 166, DefaultSerializer),
        (classOf[GenesisObservation], 167, DefaultSerializer),
        (classOf[LastTransactionRef], 1010, DefaultSerializer),
        (classOf[ObservationData], 168, DefaultSerializer),
        (classOf[CheckpointBlockWithMissingParents], 169, DefaultSerializer),
        (classOf[CheckpointBlockWithMissingSoe], 170, DefaultSerializer),
        (classOf[RequestTimeoutOnConsensus], 171, DefaultSerializer),
        (classOf[RequestTimeoutOnResolving], 172, DefaultSerializer),
        (classOf[CheckpointBlockInvalid], 173, DefaultSerializer),
        (classOf[Observation], 174, DefaultSerializer),
        (classOf[scala.collection.mutable.ArrayBuffer[String]], 175, DefaultSerializer),
        (classOf[Set[String]], 176, DefaultSerializer),
        (classOf[Id], 1011, DefaultSerializer),
        (classOf[Array[Byte]], 1012, DefaultSerializer),
        (classOf[Array[Array[Byte]]], 177, DefaultSerializer),
        (classOf[Option[Long]], 1013, DefaultSerializer),
        (classOf[String], 1014, DefaultSerializer),
        (classOf[Boolean], 1015, DefaultSerializer),
        (Class.forName("[[I"), 182, DefaultSerializer), // To make int[][] serializable
        (classOf[AddressMetaData], 186, DefaultSerializer),
        (Class.forName("scala.math.LowPriorityOrderingImplicits$$anon$3"), 188, DefaultSerializer),
        (Class.forName("scala.Predef$$anon$2"), 189, DefaultSerializer),
        (EdgeHashType.AddressHash.getClass, 1024, DefaultSerializer),
        (EdgeHashType.CheckpointDataHash.getClass, 1025, DefaultSerializer),
        (EdgeHashType.CheckpointHash.getClass, 1026, DefaultSerializer),
        (EdgeHashType.TransactionDataHash.getClass, 1027, DefaultSerializer),
        (EdgeHashType.TransactionHash.getClass, 1028, DefaultSerializer),
        (EdgeHashType.ValidationHash.getClass, 1029, DefaultSerializer),
        (EdgeHashType.BundleDataHash.getClass, 1030, DefaultSerializer),
        (EdgeHashType.ChannelMessageDataHash.getClass, 1031, DefaultSerializer),
        (Class.forName("scala.Enumeration$Val"), 1017, DefaultSerializer),
        (Class.forName("scala.collection.immutable.HashSet$HashSet1"), 135, DefaultSerializer),
        (Class.forName("scala.collection.immutable.Set$EmptySet$"), 131, DefaultSerializer),
        (Class.forName("scala.collection.IndexedSeqLike$Elements"), 190, DefaultSerializer),
        (Class.forName("scala.collection.immutable.$colon$colon"), 1020, DefaultSerializer),
        (Class.forName("scala.None$"), 114, DefaultSerializer),
        (Class.forName("scala.collection.immutable.Nil$"), 116, DefaultSerializer),
        (Class.forName("scala.collection.immutable.Map$EmptyMap$"), 136, DefaultSerializer),
        (classOf[SnapshotProposalPayload], 200, DefaultSerializer),
        (classOf[Signed[_]], 201, DefaultSerializer),
        (classOf[SnapshotProposal], 202, DefaultSerializer),
        (classOf[MajorityInfo], 203, DefaultSerializer),
        (classOf[HeightRange], 204, DefaultSerializer),
        (classOf[CheckpointBlockPayload], 205, DefaultSerializer),
        (classOf[FinishedCheckpointBlock], 206, DefaultSerializer),
        (classOf[SnapshotInfo], 1034, CompatibleSerializer)
      )
    ) {}
