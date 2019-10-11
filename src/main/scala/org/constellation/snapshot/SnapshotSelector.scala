package org.constellation.snapshot

import org.constellation.domain.schema.Id
import org.constellation.storage.{RecentSnapshot, SnapshotVerification}
import org.constellation.util.SnapshotDiff

case class DownloadInfo(diff: SnapshotDiff, recentStateToSet: List[RecentSnapshot])

trait SnapshotSelector {

  type NodeSnapshots = (Id, List[RecentSnapshot])

  def selectSnapshotFromRecent(
    peersSnapshots: List[NodeSnapshots],
    ownSnapshots: List[RecentSnapshot]
  ): Option[DownloadInfo]

  def selectSnapshotFromBroadcastResponses(
    responses: List[Option[SnapshotVerification]],
    ownSnapshots: List[RecentSnapshot]
  ): Option[DownloadInfo]

  private[snapshot] def createDiff(
    major: List[RecentSnapshot],
    ownSnapshots: List[RecentSnapshot],
    peers: List[Id]
  ): SnapshotDiff =
    SnapshotDiff(
      ownSnapshots.diff(major).sortBy(_.height).reverse,
      major.diff(ownSnapshots).sortBy(_.height).reverse,
      peers
    )

}
