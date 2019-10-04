package org.constellation.snapshot

import cats.implicits._
import org.constellation.domain.schema.Id
import org.constellation.storage.{RecentSnapshot, SnapshotVerification, VerificationStatus}

class HeightIdBasedSnapshotSelector(thisNodeId: Id, snapshotHeightRedownloadDelayInterval: Int)
    extends SnapshotSelector {

  /**
    * Selects snapshots at given height and validates against current, if the current snapshot is incorrect highest one will be chosen.
    * @param peersSnapshots cluster recent snapshots
    * @param ownSnapshots own recent snapshots
    * @return None when current highest snapshot matches one selected from cluster.
    *         Some(..) downloadInfo with recentSnapshots list downloaded from highest node based on majority of hashes and id election.
    */
  def selectSnapshotFromRecent(
    peersSnapshots: List[NodeSnapshots],
    ownSnapshots: List[RecentSnapshot]
  ): Option[DownloadInfo] =
    (peersSnapshots, ownSnapshots) match {
      case (_, Nil) =>
        val clusterWithCorrectState = selectMostRecentCorrectSnapshot(peersSnapshots)
        DownloadInfo(
          createDiff(clusterWithCorrectState._1, ownSnapshots, clusterWithCorrectState._2),
          clusterWithCorrectState._1
        ).some
      case (Nil, _) => None
      case (_, _) =>
        val highestSnapshot = ownSnapshots.maxBy(_.height)
        val correctSnapAtGivenHeight = selectCorrectRecentSnapshotAtGivenHeight(highestSnapshot, peersSnapshots)
        val clusterWithCorrectState = selectMostRecentCorrectSnapshot(peersSnapshots :+ (thisNodeId, ownSnapshots))
        if (correctSnapAtGivenHeight._1 == highestSnapshot && !isBelowInterval(
              highestSnapshot,
              clusterWithCorrectState._1
            ))
          None
        else {
          DownloadInfo(
            createDiff(clusterWithCorrectState._1, ownSnapshots, clusterWithCorrectState._2),
            clusterWithCorrectState._1
          ).some
        }
    }

  /**
    * Handles cluster responses for newest broadcasted snapshot.
    * @param responses cluster responses
    * @param ownSnapshots own recent snapshots
    * @return None when correct clusterState are higher than invalid.
    *         None when invalid equals correct but at given height same RecentSnapshot is being return due to id sorting election.
    *         Some(..) downloadInfo with recentSnapshots list downloaded from highest node based on majority of hashes and id election.
    */
  def selectSnapshotFromBroadcastResponses(
    responses: List[Option[SnapshotVerification]],
    ownSnapshots: List[RecentSnapshot]
  ): Option[DownloadInfo] = {
    val flat = responses.flatten
    val grouped = flat.groupBy(_.status)

    val invalid = grouped.getOrElse(VerificationStatus.SnapshotInvalid, List.empty)
    val correct = grouped.getOrElse(VerificationStatus.SnapshotCorrect, List.empty)
    // +1 because this node treat it as correct
    val maybeSelected = (invalid.size, correct.size + 1) match {
      case (i, c) if i > c =>
        selectMostRecentCorrectSnapshot(invalid.map(s => (s.id, s.recentSnapshot))).some
      case (i, c) if i < c => None
      case (i, c) if i == c =>
        val combined = invalid ++ correct
        val highestSnapshot = ownSnapshots.maxBy(_.height)
        val correctSnapAtGivenHeight = selectCorrectRecentSnapshotAtGivenHeight(
          highestSnapshot,
          combined.map(x => (x.id, x.recentSnapshot))
        )
        val info =
          ((invalid ++ correct).map(s => (s.id, s.recentSnapshot)) :+ (thisNodeId, ownSnapshots)).filter(_._2.nonEmpty)
        if (info.nonEmpty) {
          val clusterWithCorrectState = selectMostRecentCorrectSnapshot(info)
          if (correctSnapAtGivenHeight._1 == highestSnapshot && !isBelowInterval(
                highestSnapshot,
                clusterWithCorrectState._1
              )) None
          else clusterWithCorrectState.some
        } else {
          None
        }
    }

    maybeSelected.map { s =>
      DownloadInfo(createDiff(s._1, ownSnapshots, s._2), s._1)
    }
  }

  /**
    * Selects correct cluster state at given height only, on multiple results election is made by popularity and id sorting
    * @param ownSnapshot
    * @param clusterState
    * @return elected majority state at given height and list of nodes associated with that state
    */
  private[snapshot] def selectCorrectRecentSnapshotAtGivenHeight(
    ownSnapshot: RecentSnapshot,
    clusterState: List[NodeSnapshots]
  ): (RecentSnapshot, List[Id]) =
    (clusterState
      .flatMap(n => n._2.find(_.height == ownSnapshot.height).map((n._1, _))) :+ (thisNodeId, ownSnapshot))
      .groupBy(_._2)
      .toList
      .maxBy(t => (t._2.size, t._2.map(_._1.hex)))
      .map(x => x.map(_._1))

  /**
    *  Selects correct cluster state based on most recent (highest) snapshots, on multiple results election is made by popularity and id sorting
    * @param clusterState
    * @return elected majority state and list of nodes associated with that state
    */
  private[snapshot] def selectMostRecentCorrectSnapshot(
    clusterState: List[NodeSnapshots]
  ): (List[RecentSnapshot], List[Id]) = {
    val x = clusterState
      .map(r => (r, r._2.maxBy(_.height)))
      .groupBy(_._2)
      .toList
      .maxBy(t => (t._1.height, t._2.size, t._2.map(_._1._1.hex)))
      ._2

    (x.head._1._2, x.map(_._1._1))
  }

  private def isBelowInterval(ownSnapshots: RecentSnapshot, snapshotsToDownload: List[RecentSnapshot]) =
    (ownSnapshots.height + snapshotHeightRedownloadDelayInterval) < (snapshotsToDownload match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    })

}
