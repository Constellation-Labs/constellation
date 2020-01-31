package org.constellation.snapshot

import cats.effect.Concurrent
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.Id
import org.constellation.storage.VerificationStatus.VerificationStatus
import org.constellation.storage.{RecentSnapshot, SnapshotVerification, VerificationStatus}
import org.constellation.util.SnapshotDiff

class HeightIdBasedSnapshotSelector[F[_]: Concurrent](nodeId: Id, snapshotHeightDelayInterval: Int)
    extends SnapshotSelector[F]
    with StrictLogging {

  implicit val implicitLogger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /**
    * Selects snapshots at given height and validates against current, if the current snapshot is incorrect highest one will be chosen.
    * @param peersSnapshots cluster recent snapshots
    * @param ownSnapshots own recent snapshots
    * @return None when current highest snapshot matches one selected from cluster.
    *         Some(..) downloadInfo with recentSnapshots list downloaded from highest node based on majority of hashes and id election.
    */
  def selectSnapshotFromRecent(
    peersSnapshots: Map[Id, List[RecentSnapshot]],
    ownSnapshots: List[RecentSnapshot]
  ): Option[(SnapshotDiff, List[RecentSnapshot])] =
    (peersSnapshots, ownSnapshots) match {
      case (m, _) if m.isEmpty => None
      case (_, Nil) =>
        val nel = peersSnapshots.filter(_._2.nonEmpty)
        if (nel.isEmpty) None
        else {
          logger.debug(s"selectSnapshotFromRecent - !nel.isEmpty - selectMostRecentCorrectSnapshot with nel: ${nel}")
          val state = selectMostRecentCorrectSnapshot(nel)
          (createDiff(state._1, ownSnapshots, state._2), state._1).some
        }
      case (_, _) =>
        val highestSnapshot = ownSnapshots.maxBy(_.height)
        val correctSnapAtGivenHeight = selectCorrectRecentSnapshotAtGivenHeight(highestSnapshot, peersSnapshots)
        logger.debug(s"selectSnapshotFromRecent - correctSnapAtGivenHeight: ${correctSnapAtGivenHeight}")
        val nel = (peersSnapshots + (nodeId -> ownSnapshots)).filter(_._2.nonEmpty)
        if (nel.isEmpty) None
        else {
          val clusterWithCorrectState = selectMostRecentCorrectSnapshot(nel)
          logger.debug(s"selectSnapshotFromRecent - !nel.isEmpty - clusterWithCorrectState: ${clusterWithCorrectState}")
          val check = correctSnapAtGivenHeight._1 == highestSnapshot && !isBelowInterval(
            highestSnapshot,
            clusterWithCorrectState._1
          )
          if (check) {
            logger.debug(s"selectSnapshotFromRecent - check")
            None
          } else {
            val res = (
              createDiff(clusterWithCorrectState._1, ownSnapshots, clusterWithCorrectState._2),
              clusterWithCorrectState._1
            ).some
            logger.debug(s"selectSnapshotFromRecent - !check - ${res}")
            res
          }
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
  ): Option[(SnapshotDiff, List[RecentSnapshot])] = {
    val grouped = responses.flatten.groupBy(_.status)

    def getByStatus(status: VerificationStatus) =
      grouped.getOrElse(status, List.empty).map(s => (s.id -> s.recentSnapshot)).toMap

    val above = getByStatus(VerificationStatus.SnapshotHeightAbove)
    val invalid = getByStatus(VerificationStatus.SnapshotInvalid)
    val correct = getByStatus(VerificationStatus.SnapshotCorrect)

    val maybeSelected =
      (invalid.size, correct.size + 1, above.size) match { // +1 because own node treats it as correct too
        case (i, c, a) if a > (i + c) => None
        case (i, c, _) if i > c =>
          logger.debug(s"selectSnapshotFromBroadcastResponses - (i, c) if i > c => - invalid: ${invalid}")
          selectMostRecentCorrectSnapshot(invalid).some
        case (i, c, _) if i < c => None
        case (i, c, _) if i == c =>
          val combined = invalid ++ correct
          val highestSnapshot = ownSnapshots.maxBy(_.height)
          val correctSnapshotAtGivenHeight = selectCorrectRecentSnapshotAtGivenHeight(
            highestSnapshot,
            combined
          )
          val info = (invalid ++ correct + (nodeId -> ownSnapshots)).filter(_._2.nonEmpty)

          if (info.nonEmpty) {
            logger.debug(s"selectSnapshotFromBroadcastResponses - info.nonEmpty - info: ${info}")
            val clusterWithCorrectState = selectMostRecentCorrectSnapshot(info)
            val check = correctSnapshotAtGivenHeight._1 == highestSnapshot && !isBelowInterval(
              highestSnapshot,
              clusterWithCorrectState._1
            )
            if (check) {
              logger.debug(s"selectSnapshotFromBroadcastResponses - check - info: ${info}")
              None
            } else {
              logger.debug(s"selectSnapshotFromBroadcastResponses - !check - info: ${info}")
              clusterWithCorrectState.some
            }
          } else None
      }

    maybeSelected.map { s =>
      (createDiff(s._1, ownSnapshots, s._2), s._1)
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
    clusterState: Map[Id, List[RecentSnapshot]]
  ): (RecentSnapshot, List[Id]) =
    (clusterState.mapFilter(_.find(_.height == ownSnapshot.height)) + (nodeId -> ownSnapshot)).groupBy {
      case (_, snapshot) => snapshot
    }.mapValues(_.keys.toList).maxBy {
      case (snapshot, ids) => (weightByTrustProposers(List(snapshot), ids), ids.map(_.hex))
    }

  /**
    *  Selects correct cluster state based on most recent (highest) snapshots, on multiple results election is made by popularity and id sorting
    * @param clusterState
    * @return elected majority state and list of nodes associated with that state
    */
  private[snapshot] def selectMostRecentCorrectSnapshot(
    clusterState: Map[Id, List[RecentSnapshot]]
  ): (List[RecentSnapshot], List[Id]) = {
    logger.debug(s"selectSnapshotFromBroadcastResponses - info: ${clusterState}")

    val res = clusterState.groupBy { case (_, snapshots) => snapshots.maxBy(_.height) }
      .mapValues(m => (m.values.head, m.keys))
      .values
      .maxBy {
        case (snapshots, ids) => (snapshots.head.height, weightByTrustProposers(snapshots, ids.toList), ids.map(_.hex))
      }
      .map(_.toList)
    logger.debug(s"selectMostRecentCorrectSnapshot: ${res}")
    res
  }

  def weightByTrustProposers(snapshots: List[RecentSnapshot], ids: List[Id]): Double = {
    val proposedTrustViews = snapshots.map(_.publicReputation).combineAll

    ids.map(proposedTrustViews.getOrElse(_, 0d)).sum
  }

  private def isBelowInterval(ownSnapshots: RecentSnapshot, snapshotsToDownload: List[RecentSnapshot]) =
    (ownSnapshots.height + snapshotHeightDelayInterval) < (snapshotsToDownload match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    })

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

case class DownloadInfo(diff: SnapshotDiff, recentStateToSet: List[RecentSnapshot])
