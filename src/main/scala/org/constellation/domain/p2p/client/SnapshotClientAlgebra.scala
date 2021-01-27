package org.constellation.domain.p2p.client

import org.constellation.domain.redownload.RedownloadService.{SnapshotProposalsAtHeight, SnapshotsAtHeight}
import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.Id
import org.constellation.schema.snapshot.{LatestMajorityHeight, SnapshotProposalPayload}

trait SnapshotClientAlgebra[F[_]] {
  def getStoredSnapshots(): PeerResponse[F, List[String]]

  def getStoredSnapshot(hash: String): PeerResponse[F, Array[Byte]]

  def getCreatedSnapshots(): PeerResponse[F, SnapshotProposalsAtHeight]

  def getAcceptedSnapshots(): PeerResponse[F, SnapshotsAtHeight]

  def getPeerProposals(id: Id): PeerResponse[F, Option[SnapshotProposalsAtHeight]]

  def getNextSnapshotHeight(): PeerResponse[F, (Id, Long)]

  def getSnapshotInfo(): PeerResponse[F, Array[Byte]]

  def getSnapshotInfo(hash: String): PeerResponse[F, Array[Byte]]

  def getLatestMajorityHeight(): PeerResponse[F, LatestMajorityHeight]

  def postPeerProposal(message: GossipMessage[SnapshotProposalPayload]): PeerResponse[F, Unit]
}
