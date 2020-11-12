package org.constellation

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.schema.v2.{Id, NodeState, NodeType}
import org.constellation.util._

case class PeerMetadata(
  host: String,
  httpPort: Int,
  id: Id,
  alias: Option[String] = None,
  nodeState: NodeState = NodeState.Ready,
  timeAdded: Long = System.currentTimeMillis(),
  auxHost: String = "",
  auxAddresses: Seq[String] = Seq(), // for testing multi key address partitioning
  nodeType: NodeType = NodeType.Full,
  resourceInfo: ResourceInfo
) {
  def toPeerClientMetadata: PeerClientMetadata = PeerClientMetadata(host, httpPort, id)
}

object PeerMetadata {
  implicit val peerMetadataEncoder: Encoder[PeerMetadata] = deriveEncoder
  implicit val peerMetadataDecoder: Decoder[PeerMetadata] = deriveDecoder
}

case class ResourceInfo(
  maxMemory: Long = Runtime.getRuntime.maxMemory(),
  cpuNumber: Int = Runtime.getRuntime.availableProcessors(),
  diskUsableBytes: Long
)

object ResourceInfo {
  implicit val resourceInfoEncoder: Encoder[ResourceInfo] = deriveEncoder
  implicit val resourceInfoDecoder: Decoder[ResourceInfo] = deriveDecoder
}

case class RemovePeerRequest(host: Option[HostPort] = None, id: Option[Id] = None)

case class UpdatePassword(password: String)

object ProcessingConfig {

  val testProcessingConfig = ProcessingConfig(
    maxWidth = 10,
    maxMemPoolSize = 1000,
    minPeerTimeAddedSeconds = 5,
    roundsPerMessage = 1,
    leavingStandbyTimeout = 3
  )
}

case class ProcessingConfig(
  maxWidth: Int = 10,
  maxTipUsage: Int = 2,
  maxTXInBlock: Int = 50,
  maxMessagesInBlock: Int = 1,
  peerInfoTimeout: Int = 3,
  snapshotTriggeringTimeSeconds: Int = 5,
  redownloadPeriodicCheckTimeSeconds: Int = 30,
  formUndersizedCheckpointAfterSeconds: Int = 30,
  numFacilitatorPeers: Int = 2,
  metricCheckInterval: Int = 10,
  maxMemPoolSize: Int = 35000,
  minPeerTimeAddedSeconds: Int = 30,
  maxActiveTipsAllowedInMemory: Int = 1000,
  maxAcceptedCBHashesInMemory: Int = 50000,
  peerHealthCheckInterval: Int = 30,
  peerDiscoveryInterval: Int = 60,
  formCheckpointTimeout: Int = 60,
  roundsPerMessage: Int = 10,
  maxInvalidSnapshotRate: Int = 51,
  txGossipingFanout: Int = 2,
  leavingStandbyTimeout: Int = 30
) {}

case class ChannelUIOutput(channels: Seq[String])

case class ChannelValidationInfo(channel: String, valid: Boolean)

object ChannelValidationInfo {
  implicit val channelValidationInfoEncoder: Encoder[ChannelValidationInfo] = deriveEncoder
}

case class BlockUIOutput(
  id: String,
  height: Long,
  parents: Seq[String],
  channels: Seq[ChannelValidationInfo]
)

object BlockUIOutput {
  implicit val blockUIOutputEncoder: Encoder[BlockUIOutput] = deriveEncoder
}
