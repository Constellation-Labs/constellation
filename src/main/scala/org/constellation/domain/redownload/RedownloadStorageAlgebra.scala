package org.constellation.domain.redownload

import org.constellation.domain.redownload.RedownloadService.{
  PeersProposals,
  Reputation,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signed
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo, SnapshotProposal}

trait RedownloadStorageAlgebra[F[_]] {
  def getCreatedSnapshots: F[SnapshotProposalsAtHeight]
  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit]
  def updateCreatedSnapshots(plan: RedownloadPlan): F[Unit]

  def getAcceptedSnapshots: F[SnapshotsAtHeight]
  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit]
  def updateAcceptedSnapshots(plan: RedownloadPlan): F[Unit]

  def getPeersProposals: F[Map[Id, SnapshotProposalsAtHeight]]
  def getPeerProposals(peer: Id): F[Option[SnapshotProposalsAtHeight]]
  def replacePeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit]
  def persistPeerProposal(peer: Id, proposal: Signed[SnapshotProposal]): F[Unit]
  def persistPeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit]

  def getLastMajorityState: F[SnapshotsAtHeight]
  def setLastMajorityState(majorityState: SnapshotsAtHeight): F[Unit]

  def getLastSentHeight: F[Long]
  def setLastSentHeight(height: Long): F[Unit]

  def getLatestMajorityHeight: F[Long]
  def getLowestMajorityHeight: F[Long]
  def getMajorityRange: F[HeightRange]
  def getMajorityGapRanges: F[List[HeightRange]]

  def getPeerMajorityInfo: F[Map[Id, MajorityInfo]]
  def updatePeerMajorityInfo(peerId: Id, majorityInfo: MajorityInfo): F[Unit]

  def clear(): F[Unit]

  def minHeight[V](snapshots: Map[Long, V]): Long
  def maxHeight[V](snapshots: Map[Long, V]): Long
  def getRemovalPoint(maxHeight: Long): Long
  def getIgnorePoint(maxHeight: Long): Long
  def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V]

}
