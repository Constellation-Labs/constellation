package org.constellation.domain.p2p.client

import org.constellation.domain.redownload.RedownloadService.{
  LatestMajorityHeight,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.Id

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
}
