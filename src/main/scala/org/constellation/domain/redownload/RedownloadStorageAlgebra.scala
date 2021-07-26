package org.constellation.domain.redownload

import org.constellation.concurrency.cuckoo.CuckooFilter
import org.constellation.domain.redownload.RedownloadService.{
  PeersProposals,
  Reputation,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.schema.Id
import org.constellation.schema.signature.Signed
import org.constellation.schema.snapshot.{FilterData, HeightRange, SnapshotProposal}
import org.constellation.storage.JoinActivePoolCommand

trait RedownloadStorageAlgebra[F[_]] {
  def getCreatedSnapshots: F[SnapshotProposalsAtHeight]
  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit]
  def updateCreatedSnapshots(plan: RedownloadPlan): F[Unit]

  def getAcceptedSnapshots: F[SnapshotsAtHeight]
  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit]
  def updateAcceptedSnapshots(plan: RedownloadPlan): F[Unit]

  def removeSnapshotsAndProposalsBelowHeight(
    height: Long
  ): F[(SnapshotsAtHeight, SnapshotProposalsAtHeight, PeersProposals)]

  def getPeersProposals: F[Map[Id, SnapshotProposalsAtHeight]]
  def getPeerProposals(peer: Id): F[Option[SnapshotProposalsAtHeight]]
  def replacePeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit]
  def persistPeerProposal(proposal: Signed[SnapshotProposal]): F[Unit]
  def persistPeerProposals(proposals: Iterable[Signed[SnapshotProposal]]): F[Unit]

  def getLastMajorityState: F[SnapshotsAtHeight]
  def setLastMajorityState(majorityState: SnapshotsAtHeight): F[Unit]

  def getLastSentHeight: F[Long]
  def setLastSentHeight(height: Long): F[Unit]

  def getLatestMajorityHeight: F[Long]
  def getLowestMajorityHeight: F[Long]
  def getMajorityRange: F[HeightRange]

  def clear(): F[Unit]

  def minHeight[V](snapshots: Map[Long, V]): Long
  def maxHeight[V](snapshots: Map[Long, V]): Long

  def getRemoteFilters: F[Map[Id, CuckooFilter]]
  def replaceRemoteFilterData(peerId: Id, filterData: FilterData): F[Unit]
  def localFilterData: F[FilterData]

  def getMajorityStallCount: F[Int]
  def resetMajorityStallCount: F[Unit]
  def incrementMajorityStallCount: F[Unit]

  def addJoinActivePoolCommand(senderId: Id, command: JoinActivePoolCommand): F[Map[Id, JoinActivePoolCommand]]
  def clearJoinActivePoolCommands(): F[Unit]

}
