package org.constellation.util

import cats.data.OptionT
import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.primitives.Schema._
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot

import scala.util.Random

class MajorityStateChooser[F[_]: Concurrent] {

  type NodeSnapshots = (Id, Seq[RecentSnapshot])
  type SnapshotNodes = (RecentSnapshot, Seq[Id])

  val logger = Slf4jLogger.getLogger[F]

  private final val differenceInSnapshotHeightToReDownloadFromLeader = 10

  def chooseMajorityState(
    nodeSnapshots: List[NodeSnapshots],
    ownHeight: Long
  ): OptionT[F, (Seq[RecentSnapshot], Set[Id])] =
    for {
      majorState <- chooseMajoritySnapshot(
        nodeSnapshots.filter(checkIfNodeContainsSnapshotsInConsistentState),
        ownHeight
      )
      nodeId <- OptionT.fromOption[F](chooseNodeId(majorState))
      node: (Id, Seq[RecentSnapshot]) <- OptionT.fromOption[F](findNode(nodeSnapshots, nodeId))
      _ <- OptionT.liftF(logger.debug(s"Re-download from node : ${node}"))
    } yield (node._2, Set(node._1))

  private def chooseMajoritySnapshot(nodeSnapshots: Seq[NodeSnapshots], ownHeight: Long) =
    for {
      // highestSnapshot <- OptionT.fromOption[F](getHighest(nodeSnapshots))
      // useHighest <- OptionT.liftF(shouldUseHighest(highestSnapshot, ownHeight))
      majorSnapshot <- OptionT.fromOption[F](chooseMajor(nodeSnapshots))

      _ <- OptionT.liftF(
        logger.debug(
//          s"The highest snapshot : $highestSnapshot"
          s"The major snapshot: $majorSnapshot"
        )
      )
    } yield majorSnapshot

  private def dropToCurrentState(nodeSnapshot: NodeSnapshots, major: SnapshotNodes): (Seq[RecentSnapshot], Set[Id]) =
    (nodeSnapshot._2.sortBy(-_.height).dropWhile(_ != major._1), Set(nodeSnapshot._1))

  private def findNode(nodeSnapshots: Seq[NodeSnapshots], nodeId: Id) =
    nodeSnapshots.find(_._1 == nodeId)

  private def chooseNodeId(snapshotNodes: SnapshotNodes) =
    Random.shuffle(snapshotNodes._2) match {
      case Nil    => None
      case x :: _ => Some(x)
    }

  private def shouldUseHighest(snapshotNodes: SnapshotNodes, ownHeight: Long) =
    ((snapshotNodes._1.height - ownHeight) >= differenceInSnapshotHeightToReDownloadFromLeader).pure[F]

  private def chooseMajor(nodeSnapshots: Seq[NodeSnapshots]) =
    sortByHeightAndHash(nodeSnapshots).filter(_._2.lengthCompare(nodeSnapshots.count(_._2.nonEmpty) / 2) > 0) match {
      case Nil    => None
      case x :: _ => Some(x)
    }

  private def getHighest(nodeSnapshots: Seq[NodeSnapshots]) =
    sortByHeightAndHash(nodeSnapshots) match {
      case Nil    => None
      case x :: _ => Some(x)
    }

  private def sortByHeightAndHash(nodeSnapshots: Seq[NodeSnapshots]) =
    nodeSnapshots
      .flatMap {
        case (id, snapshots) => snapshots.map((id, _))
      }
      .groupBy {
        case (_, snapshot) => snapshot
      }
      .toList
      .sortBy {
        case (snapshot, _) => snapshot.height
      }
      .map {
        case (snapshot, idsMap) => (snapshot, idsMap.map(_._1))
      }

  private def checkIfNodeContainsSnapshotsInConsistentState(nodeSnapshots: NodeSnapshots) = {
    val nodeSnapshotsHeights: Seq[Long] = nodeSnapshots._2.map(a => a.height)
    val shouldContainsSnapshotsWithHeights: Set[Long] =
      Seq.range(minOrZero(nodeSnapshotsHeights.toList), maxOrZero(nodeSnapshotsHeights.toList) + 1).filter(isEven).toSet

    shouldContainsSnapshotsWithHeights.subsetOf(nodeSnapshotsHeights.toSet)
  }

  private def isEven(number: Long) = number % 2 == 0

  private def maxOrZero(list: List[Long]): Long =
    list match {
      case Nil => 0
      case _   => list.max
    }

  private def minOrZero(list: List[Long]): Long =
    list match {
      case Nil => 0
      case _   => list.min
    }
}
