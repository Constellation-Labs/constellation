package org.constellation.util

import cats.data.OptionT
import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot

import scala.util.Try

object MajorityStateChooser {
  type NodeSnapshots = (Id, Seq[RecentSnapshot])
  type SnapshotNodes = (RecentSnapshot, Seq[Id])

  def chooseMajNodeIds(snapshotNodes: SnapshotNodes, nodeSnapshots: List[NodeSnapshots]): Option[Seq[Id]] =
    nodeSnapshots
      .filter(ns => snapshotNodes._2.contains(ns._1))
      .sortBy(-_._2.size)
      .map(_._1) match {
      case Nil                   => None
      case majNodIds @ (id :: _) => Some(majNodIds)
    }

  def selectMajSnap(totalPeers: Int, allSnapshotNodes: Seq[SnapshotNodes]) = {
    val numNodesWithSnapAtThisHeight = allSnapshotNodes.flatMap(_._2).length
    val snapVotes: Seq[(RecentSnapshot, Int)] = allSnapshotNodes.map { case (recSnap, ids) => (recSnap, ids.length) }
    val simpleMaj: Option[RecentSnapshot] = snapVotes.find { case (recSnap, count) => count - (totalPeers / 2) >= 0 }
      .map(_._1)
    if (simpleMaj.isDefined) simpleMaj
    else if (totalPeers == numNodesWithSnapAtThisHeight) { //todo need actual logic to get majority from a distribution. This could be improved
      val votedMajority: Option[RecentSnapshot] = snapVotes.sortBy { case (recSnap, count) => (count, recSnap.hash) }
        .map(_._1)
        .headOption
      votedMajority
    } else None
  }

  def getAllSnapsUntilMaj(snapshotNodes: SnapshotNodes, nodeSnapshots: List[NodeSnapshots]) = {
    val nodeWithMajority = nodeSnapshots.filter { case (id, snaps) => snapshotNodes._2.contains(id) }
      .maxBy(snaps => snaps._2.maxBy(snap => snap.height).height)
    nodeWithMajority._2.filter(_.height <= snapshotNodes._1.height)
  }

  def chooseMajorWinner(allPeers: Seq[Id], nodeSnapshots: Seq[NodeSnapshots]) = { //todo instantiate with total peers
    val snapshotToProposers: Seq[(RecentSnapshot, Seq[(Id, RecentSnapshot)])] = nodeSnapshots.flatMap {
      case (id, snapshots) => snapshots.map((id, _))
    }.groupBy {
      case (_, snapshot) => snapshot
    }.toList
    val snapsGroupedByHeight: Map[Long, Seq[(RecentSnapshot, Seq[Id])]] = snapshotToProposers.map {
      case (recentSnap, idRecentSnapTups) => (recentSnap, idRecentSnapTups.map(_._1))
    }.groupBy(_._1.height)
    val res = Try {
      snapsGroupedByHeight.flatMap {
        case (height, allSnapshotNodes) =>
          val getMajOpt = selectMajSnap(allPeers.length, allSnapshotNodes)
          allSnapshotNodes.find(sn => getMajOpt.contains(sn._1)).map(maj => (height, maj))
      }.maxBy(_._1)
    }.toOption
    res
  }
}

class MajorityStateChooser[F[_]: Concurrent] {
  import MajorityStateChooser._

  val logger = Slf4jLogger.getLogger[F]

  private final val differenceInSnapshotHeightToReDownloadFromLeader = 10

  def chooseMajorityState(
    nodeSnapshots: List[NodeSnapshots],
    ownHeight: Long,
    allPeers: Seq[Id]
  ): OptionT[F, (Seq[RecentSnapshot], Set[Id])] =
    for {
      majorState <- chooseMajoritySnapshot(
        nodeSnapshots.filter(checkIfNodeContainsSnapshotsInConsistentState),
        ownHeight,
        allPeers
      )
      snapsThoughMaj <- OptionT.fromOption[F](Some(getAllSnapsUntilMaj(majorState._2, nodeSnapshots)))
//      node <- OptionT.fromOption[F](findNode(nodeSnapshots, nodeId))
      nodeIds <- OptionT.fromOption[F](chooseMajNodeIds(majorState._2, nodeSnapshots))

      _ <- OptionT.liftF(logger.debug(s"Re-download from nodes : ${nodeIds}"))
    } yield (snapsThoughMaj.sortBy(-_.height), nodeIds.toSet)

  private def chooseMajoritySnapshot(nodeSnapshots: Seq[NodeSnapshots], height: Long, allPeers: Seq[Id]) =
    for {
      // highestSnapshot <- OptionT.fromOption[F](getHighest(nodeSnapshots))
      // useHighest <- OptionT.liftF(shouldUseHighest(highestSnapshot, ownHeight))
      majorSnapshot <- OptionT.fromOption[F](chooseMajorWinner(allPeers, nodeSnapshots)) //def chooseMajorWinner(allPeers: Seq[Id])(nodeSnapshots: Seq[NodeSnapshots])

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

  private def chooseNodeId(snapshotNodes: SnapshotNodes, nodeSnapshots: List[NodeSnapshots]) =
    nodeSnapshots.filter(ns => snapshotNodes._2.contains(ns._1)).sortBy(-_._2.size).headOption.map(_._1)

  def shouldUseHighest(snapshotNodes: SnapshotNodes, ownHeight: Long) =
    ((snapshotNodes._1.height - ownHeight) >= differenceInSnapshotHeightToReDownloadFromLeader).pure[F]

  private def chooseMajor(nodeSnapshots: Seq[NodeSnapshots]) =
    sortByHeightAndHash(nodeSnapshots).filter {
      case (recentSnap, ids) =>
        val nodeWithSnapAtThisHeight = nodeSnapshots.count(ns => ns._2.nonEmpty)
        val comp = ids.length - (nodeWithSnapAtThisHeight / 2) >= 0
        comp
    } match {
      case Nil    => None
      case x :: _ => Some(x)
    }

  private def getHighest(nodeSnapshots: Seq[NodeSnapshots]) =
    sortByHeightAndHash(nodeSnapshots) match {
      case Nil    => None
      case x :: _ => Some(x)
    }

  private def sortByHeightAndHash(nodeSnapshots: Seq[NodeSnapshots]) =
    nodeSnapshots.flatMap {
      case (id, snapshots) => snapshots.map((id, _))
    }.groupBy {
      case (_, snapshot) => snapshot
    }.toList.sortBy {
      case (snapshot, _) => -snapshot.height
    }.map {
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
